"""Run after cargo build --workspace --bins: python3 tests/local_clients.py /path/to/debug /path/to/uw-agent."""
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

BIN = Path(sys.argv.pop(1)).resolve()
AGENT_SCRIPT = Path(sys.argv.pop(1)).resolve()


class UnixHttp(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=2)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(2)
        self.sock.connect(str(self.path))


class LocalClients(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = {'HOME': str(self.root), 'PATH': os.environ['PATH'],
                    'UW_DB_PATH': str(self.root / '.usagewindow/usagewindow.db'),
                    'UW_LISTEN_ADDR': '127.0.0.1:0'}
        self.endpoint_file = self.root / '.usagewindow/endpoint'

    def start(self, **overrides):
        self.env.update(overrides)
        hash_file = self.root / 'password-hash'
        hash_file.write_text('$argon2id$v=19$m=19456,t=2,p=1$MDEyMzQ1Njc4OWFiY2RlZg$PKEqtV6u9NNwpXPbppTEj0jTgckkrrWnVj5oRk0V1a4')
        self.env['UW_WEB_PASSWORD_HASH_FILE'] = str(hash_file)
        daemon = subprocess.Popen([str(BIN / 'uw-daemon')], env=self.env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        def stop():
            daemon.terminate()
            daemon.communicate(timeout=5)
        self.addCleanup(stop)
        deadline = time.monotonic() + 10
        while not self.endpoint_file.exists():
            self.assertIsNone(daemon.poll(), 'daemon exited before discovery publication')
            self.assertLess(time.monotonic(), deadline, 'daemon failed to publish discovery')
            time.sleep(.02)
        self.published = json.loads(self.endpoint_file.read_text())
        self.socket = self.published['endpoint'][5:]

    def run_bin(self, name, *args, input=None, env=None):
        return subprocess.run([str(BIN / name), *args], env=env or self.env,
                              input=input, text=True, capture_output=True, timeout=3)

    def request(self, method, path, body=None):
        conn = UnixHttp(self.socket)
        conn.request(method, path, json.dumps(body) if body is not None else None,
                     {'Content-Type': 'application/json'})
        response = conn.getresponse()
        status, data = response.status, json.loads(response.read())
        conn.close()
        return status, data

    # why: every production client must use the private API with a web login enabled.
    def test_cli_mcp_and_hook_use_authenticated_daemon(self):
        self.start()
        endpoint = self.run_bin('uw', 'endpoint', '--check')
        self.assertEqual(endpoint.returncode, 0, endpoint.stderr)
        self.assertEqual(endpoint.stdout.strip(), self.published['endpoint'])
        hook = self.run_bin('uw-hook', input=json.dumps({
            'hook_event_name': 'SessionStart', 'session_id': 'hook-local',
            'cwd': '/work', 'source': 'startup'}), env={**self.env, 'UW_HOOK_PROVIDER': 'codex'})
        self.assertEqual(hook.returncode, 0, hook.stderr)
        self.assertEqual(self.request('GET', '/api/sessions/hook-local')[1]['summary']['id'], 'hook-local')
        compact = self.run_bin('uw', '--json', 'compact', 'ask', 'hook-local')
        self.assertEqual(compact.returncode, 0, compact.stderr)
        response = json.loads(compact.stdout)
        self.assertEqual(response['requests'][0]['session_id'], 'hook-local')
        mcp = self.run_bin('uw-mcp', '--stdio', input=json.dumps({
            'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
            'params': {'name': 'get_resume_state', 'arguments': {'session_id': 'hook-local'}}}) + '\n')
        self.assertEqual(mcp.returncode, 0, mcp.stderr)
        reply = json.loads(mcp.stdout)
        self.assertNotIn('error', reply, reply)
        self.assertNotEqual(reply.get('result', {}).get('isError'), True, reply)
        self.assertIn('hook-local', mcp.stdout)

    # why: uw-agent must reuse the real authenticated daemon without reading secrets or starting a private unit.
    def test_agent_uses_running_daemon_without_starting_unit(self):
        self.start()
        shims = self.root / 'shims'
        shims.mkdir()
        marker = self.root / 'systemctl-called'
        systemctl = shims / 'systemctl'
        systemctl.write_text('#!/bin/sh\necho called > "' + str(marker) + '"\nexit 42\n')
        systemctl.chmod(0o755)
        env = {**self.env, 'PATH': str(shims) + ':' + str(BIN) + ':' + self.env['PATH']}
        result = subprocess.run(['bash', str(AGENT_SCRIPT), 'daemon-start'],
                                env=env, text=True, capture_output=True, timeout=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(self.published['endpoint'], result.stderr)
        self.assertFalse(marker.exists())

    # why: a system service's data-directory socket must be found despite the agent's different runtime directory.
    def test_runtime_directory_difference_and_explicit_env_precedence(self):
        self.start()
        env = {**self.env, 'XDG_RUNTIME_DIR': str(self.root / 'agent-runtime')}
        discovered = self.run_bin('uw', 'endpoint', '--check', env=env)
        self.assertEqual(discovered.returncode, 0, discovered.stderr)
        self.assertEqual(discovered.stdout.strip(), self.published['endpoint'])
        for key in ['UW_API_URL', 'UW_DAEMON_URL']:
            explicit = self.run_bin('uw', 'endpoint', env={**env, key: 'unix:/explicit/api.sock'})
            self.assertEqual(explicit.stdout.strip(), 'unix:/explicit/api.sock')
        denied = self.run_bin('uw', 'endpoint', '--check', env={**env, 'UW_API_URL': self.published['web_url']})
        self.assertNotEqual(denied.returncode, 0)
        self.assertIn('401', denied.stderr)

    # why: a custom socket discovered from a file must survive port changes with no client URL override.
    def test_custom_socket_discovery_file_and_unix_override(self):
        private = self.root / 'custom'
        private.mkdir(mode=0o700)
        self.start(UW_SOCKET_PATH=str(private / 'api.sock'))
        env = {k: v for k, v in self.env.items() if k != 'UW_SOCKET_PATH'}
        discovered = self.run_bin('uw', 'endpoint', '--check', env=env)
        self.assertEqual(discovered.returncode, 0, discovered.stderr)
        self.assertEqual(discovered.stdout.strip(), self.published['endpoint'])
        direct = self.run_bin('uw', 'endpoint', '--check', env={**env, 'UW_API_URL': self.published['endpoint']})
        self.assertEqual(direct.returncode, 0, direct.stderr)

    # why: absence of the daemon must never delay or fail an agent hook.
    def test_hook_without_daemon_is_fast_and_successful(self):
        started = time.monotonic()
        result = self.run_bin('uw-hook', input='{"hook_event_name":"SessionStart","session_id":"absent"}')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {})
        self.assertLess(time.monotonic() - started, 1)


if __name__ == '__main__':
    unittest.main()
