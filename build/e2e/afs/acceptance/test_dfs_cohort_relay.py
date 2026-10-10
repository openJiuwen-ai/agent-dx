"""Actual Linux subprocess guards for four-role failure recording and drainage."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import dfs_cohort_relay as relay

FAKE = r'''
import json,sys,time
role,mode=sys.argv[1:]
def emit(v):print(json.dumps(v),flush=True)
if mode=='hang':time.sleep(30)
if role=='ctl':
 seen=0
 while seen<3:
  x=json.loads(sys.stdin.readline())
  if x['event']=='READY':seen+=1
 for r in ('A','B','C'):emit({'event':'START','reader_id':r})
 finals=0
 while finals<3:
  line=sys.stdin.readline()
  if not line:sys.exit(3)
  x=json.loads(line)
  if mode=='reject' and x['event']=='C_DONE':
   emit({'event':'SUMMARY','status':'BLOCKED','error':'C_DONE probe did not pass'})
   sys.exit(1)
  if x['event']=='FINAL':finals+=1
 emit({'event':'SUMMARY','status':'DATA_RECORDED'})
else:
 emit({'event':'READY','reader_id':role})
 start=sys.stdin.readline()
 if not start:sys.exit(3)
 if mode=='invalid' and role=='A':
  print('invalid diagnostic stdout',flush=True);sys.exit(2)
 if mode=='reject' and role!='A':time.sleep(.2)
 emit({'event':'C_DONE','reader_id':role})
 emit({'event':'FINAL','status':'BLOCKED' if mode=='reject' and role=='A' else 'DATA_RECORDED','reader_id':role,'tail':'complete'})
 if mode=='reject' and role=='A':sys.exit(2)
'''


class BrokenClose:
    def __init__(self, stream):self.stream=stream
    @property
    def closed(self):return self.stream.closed
    def write(self, text):return self.stream.write(text)
    def flush(self):return self.stream.flush()
    def close(self):
        self.stream.close()
        raise BrokenPipeError('injected stdin close failure')


class RelayGuards(unittest.TestCase):
    def run_case(self, mode, broken_close=False, timeout=3, drain_timeout=2):
        with tempfile.TemporaryDirectory() as tmp:
            commands={r:[sys.executable,'-u','-c',FAKE,r,mode] for r in relay.ROLES}
            original=subprocess.Popen
            children=[]
            def launch(*args,**kwargs):
                p=original(*args,**kwargs);children.append(p)
                if broken_close:p.stdin=BrokenClose(p.stdin)
                return p
            folder=Path(tmp)/'receipts'
            with patch.object(relay.subprocess,'Popen',side_effect=launch):
                result=relay.relay_commands(commands,folder,timeout=timeout,drain_timeout=drain_timeout,terminate_timeout=.5)
            events=json.loads((folder/'events.json').read_text())
            receipts={r:json.loads((folder/(r+'.result.json')).read_text()) for r in relay.ROLES}
            outputs={r:(folder/(r+'.stdout')).read_text() for r in relay.ROLES}
            self.assertTrue(all(p.poll() is not None for p in children))
            self.assertTrue(result['all_started_processes_reaped'])
            self.assertTrue(result['all_stdout_pumps_closed'])
            self.assertTrue(all(v['reaped'] for v in receipts.values()))
            self.assertEqual(json.loads((folder/'summary.json').read_text()),result)
            return result,events,outputs

    def test_success_routes_and_reaps_all_roles(self):
        result,events,_=self.run_case('pass')
        self.assertEqual(result['status'],'PASS_TRANSPORT_CLOSURE_ONLY',result)
        self.assertEqual(result['codes'],dict.fromkeys(relay.ROLES,0))
        self.assertEqual(sum(v['event']['event']=='FINAL' for v in events),3)

    def test_rejection_drains_slow_workers_and_preserves_cause(self):
        result,events,outputs=self.run_case('reject')
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['primary_error']['stage'],'coordinator_rejected')
        self.assertIn('C_DONE probe did not pass',result['primary_error']['error'])
        self.assertEqual(result['codes']['B'],0)
        self.assertEqual(result['codes']['C'],0)
        self.assertTrue(all('complete' in outputs[r] for r in ('A','B','C')))
        self.assertFalse(any(v['stage']=='terminate' for v in result['errors']))

    def test_broken_stdin_close_cannot_mask_rejection_or_skip_waits(self):
        result,_,_=self.run_case('reject',broken_close=True)
        self.assertEqual(result['primary_error']['stage'],'coordinator_rejected')
        self.assertTrue(any(v['stage']=='stdin_close' for v in result['errors']))
        self.assertEqual(set(result['codes']),set(relay.ROLES))
        self.assertEqual(result['codes']['B'],0)
        self.assertEqual(result['codes']['C'],0)

    def test_invalid_stdout_retained_as_failure_not_false_pass(self):
        result,_,outputs=self.run_case('invalid')
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['primary_error']['stage'],'decode')
        self.assertIn('invalid diagnostic stdout',outputs['A'])

    def test_timeout_terminates_and_reaps_all_cohort_members(self):
        result,_,_=self.run_case('hang',timeout=.2,drain_timeout=.2)
        self.assertEqual(result['status'],'FAIL')
        self.assertEqual(result['primary_error']['stage'],'timeout')
        self.assertTrue(all(v!=0 for v in result['codes'].values()))
        self.assertTrue(any(v['stage']=='terminate' for v in result['errors']))

    def test_pair_protocol_routes_hello_done_and_reaps_both_readers(self):
        fake = FAKE.replace("seen<3", "seen<2").replace("finals<3", "finals<2")
        fake = fake.replace("('A','B','C')", "('B','C')").replace("'C_DONE'", "'DONE'")
        fake = fake.replace("emit({'event':'SUMMARY','status':'DATA_RECORDED'})", "pass")
        fake = fake.replace("emit({'event':'READY','reader_id':role})", "emit({'event':'HELLO','reader_id':role});emit({'event':'READY','reader_id':role})")
        with tempfile.TemporaryDirectory() as tmp:
            commands = {r: [sys.executable, '-u', '-c', fake, r, 'pass'] for r in ('ctl','B','C')}
            result = relay.relay_commands(commands, Path(tmp)/'receipts', timeout=3, drain_timeout=1,
                                           terminate_timeout=.5, protocol='pair')
            self.assertEqual(result['status'], 'PASS_TRANSPORT_CLOSURE_ONLY', result)
            self.assertEqual(result['codes'], {'ctl':0,'B':0,'C':0})
            self.assertTrue(result['all_started_processes_reaped'])
            self.assertTrue(result['all_stdout_pumps_closed'])
            events = json.loads((Path(tmp)/'receipts/events.json').read_text())
            self.assertEqual(sum(x['event']['event']=='HELLO' for x in events), 2)
            self.assertEqual(sum(x['event']['event']=='DONE' for x in events), 2)

    def test_pair_partial_launch_failure_reaps_coordinator_and_reader(self):
        with tempfile.TemporaryDirectory() as tmp:
            commands = {r: [sys.executable, '-u', '-c', FAKE, r, 'hang'] for r in ('ctl','B','C')}
            commands['C'] = [str(Path(tmp)/'missing')]
            result = relay.relay_commands(commands, Path(tmp)/'receipts', timeout=2, drain_timeout=.2,
                                           terminate_timeout=.2, protocol='pair')
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['started_roles'], ['ctl','B'])
            self.assertTrue(result['all_started_processes_reaped'])
            self.assertTrue(result['all_stdout_pumps_closed'])

    def test_partial_launch_failure_reaps_started_members(self):
        with tempfile.TemporaryDirectory() as tmp:
            commands={r:[sys.executable,'-u','-c',FAKE,r,'pass'] for r in relay.ROLES}
            commands['B']=[str(Path(tmp)/'missing-executable')]
            result=relay.relay_commands(commands,Path(tmp)/'receipts',timeout=2,
                                        drain_timeout=1,terminate_timeout=.5)
            self.assertEqual(result['status'],'FAIL')
            self.assertEqual(result['started_roles'],['ctl','A'])
            self.assertIn('FileNotFoundError',result['primary_error']['error'])
            self.assertTrue(result['all_started_processes_reaped'])
            self.assertTrue(result['all_stdout_pumps_closed'])


if __name__=='__main__':unittest.main()
