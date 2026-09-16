import sys, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'example'))
class ExampleAcceptance(unittest.TestCase):
    def test_complete_example_requires_unmodified_config_and_cleanup(self):
        from contract import verify, CASES
        record={'status':'passed','cases':[{'name':n,'passed':True} for n in CASES],
                'example_sha256':'abc','installed_sha256':'abc','backend_count':0,
                'external_dependencies_alive_after_stop':True,'cleanup_errors':[]}
        verify(record)
        for key,value in [('installed_sha256','changed'),('backend_count',1),
                          ('external_dependencies_alive_after_stop',False),('cleanup_errors',['failed']),('cases',[])]:
            with self.assertRaises(ValueError):verify({**record,key:value})
