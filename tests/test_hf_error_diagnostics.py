import contextlib
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from research_scheduler import hf_worker


class DiagnosticsTests(unittest.TestCase):
    def test_safe_details_keep_reason_and_limit_headers_only(self):
        e=RuntimeError('Too many files (246 files), limit 100 files. https://host/path?secret=abc hf_abc SECRET')
        e.response=types.SimpleNamespace(headers={'Retry-After':'30','RateLimit':'remaining=0','Set-Cookie':'private','Authorization':'SECRET'})
        value=hf_worker.safe_error_details(e,['SECRET'])
        self.assertIn('246 files',value['message'])
        self.assertEqual(value['headers'],{'Retry-After':'30','RateLimit':'remaining=0'})
        self.assertNotIn('SECRET',json.dumps(value));self.assertNotIn('secret=abc',json.dumps(value))
        self.assertNotIn('hf_abc',json.dumps(value))
    def test_limit_types_are_distinguished_without_error_body(self):
        for category in ('too many files','too many operations','too many requests','too many commits','too many lfs'):
            error=RuntimeError(category+' https://private.example/?token=secret')
            error.response=types.SimpleNamespace(status_code=400)
            stream=io.StringIO()
            with patch.dict('os.environ',{'RS_CONFIG_PATH':'unused'}), \
                 patch.object(Path,'read_text',return_value=json.dumps({'direction':'upload'})), \
                 patch.object(hf_worker,'upload',side_effect=error),contextlib.redirect_stdout(stream):
                with self.assertRaises(SystemExit):hf_worker.main()
            self.assertIn(category,stream.getvalue())
            self.assertNotIn('private.example',stream.getvalue())
            self.assertNotIn('secret',stream.getvalue())

    def test_http_status_without_response_secrets(self):
        error=RuntimeError('rate limit: https://signed.example/private?token=secret-value hf_secret-value')
        error.response=types.SimpleNamespace(status_code=429)
        stream=io.StringIO()
        with patch.dict('os.environ',{'RS_CONFIG_PATH':'unused'}), \
             patch.object(Path,'read_text',return_value=json.dumps({'direction':'upload'})), \
             patch.object(hf_worker,'upload',side_effect=error),contextlib.redirect_stdout(stream):
            with self.assertRaises(SystemExit):hf_worker.main()
        value=stream.getvalue()
        self.assertIn('429',value)
        self.assertIn('rate limit',value)
        self.assertNotIn('secret-value',value)
        self.assertNotIn('signed.example',value)

if __name__=='__main__':unittest.main()
