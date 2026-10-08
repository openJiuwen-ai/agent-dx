import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'firecracker'))

class TransferEvidenceTests(unittest.TestCase):
    def test_recovery_requires_new_node_generation_and_same_identity(self):
        from transfer_contract import recovered
        old = {'spec': {'id': 'i'}, 'assignment': {'node_id':'node1','generation':1}}
        new = {'spec': {'id':'i'}, 'assignment': {'node_id':'node2','generation':2},
               'result': {'state':'Running','resources_held':True}, 'recovery':{'pending':False}}
        self.assertTrue(recovered(old,new))
        for key,value in [('node_id','node1'),('generation',1)]:
            bad = {**new, 'assignment':{**new['assignment'],key:value}}
            self.assertFalse(recovered(old,bad))
        self.assertFalse(recovered(old,{**new,'spec':{'id':'other'}}))
        self.assertFalse(recovered(old,{**new,'recovery':{'pending':True}}))
        self.assertFalse(recovered(old,{**new,'result':{'state':'Failed','resources_held':False}}))

    def test_acceptance_requires_all_evidence(self):
        from transfer_contract import verify, CASES
        data = {'status':'passed','cases':[{'name':n,'passed':True} for n in CASES],
                'cleanup_errors':[], 'inventories':{'node1':0,'node2':0}}
        verify(data)
        for bad in ({**data,'cases':data['cases'][:-1]},
                    {**data,'cleanup_errors':['left running']},
                    {**data,'interruption_requested':True},
                    {**data,'inventories':{'node1':1,'node2':0}}):
            with self.assertRaises(ValueError): verify(bad)

    def test_node_restart_requires_uncommitted_backend_replacement(self):
        from transfer_contract import verify, CASES
        base={'status':'passed','cases':[{'name':n,'passed':True} for n in CASES],
              'cleanup_errors':[], 'inventories':{'node1':0,'node2':0},'node_interruption_requested':True}
        with self.assertRaises(ValueError): verify(base)
        fault={'session_before':'a','session_after':'b','backend_before':'old','backend_after':'new',
               'state_at_crash':'Paused','pending_at_crash':True,'assignment_preserved':True,'old_backend_removed':True}
        verify({**base,'node_recovery_restart':fault})
        for key,value in [('session_after','a'),('backend_after','old'),('state_at_crash','Running'),
                          ('pending_at_crash',False),('assignment_preserved',False),('old_backend_removed',False)]:
            with self.assertRaises(ValueError): verify({**base,'node_recovery_restart':{**fault,key:value}})
