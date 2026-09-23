"""Installed SDK placement acceptance; Redis is read only for observed assignments."""
import json
from adx_sandbox import Sandbox
from node import catalog


def run(connection, image, output):
    report={'status':'failed','cases':[],'instances':[],'cleanup_errors':[]}
    handles=[]
    def condition(kind,mode,key,value,**flags):
        return {'kind':kind,'affinity':mode,'labelOps':[{'type':0,'labelKey':key,'labelValues':[value]}],**flags}
    def create(**options):
        sandbox=Sandbox(image=image,runtime='runc',cpu=250,memory=256,idle_timeout=0,connection=connection,create_timeout=120,**options)
        handles.append(sandbox);report['instances'].append(sandbox.id)
        return sandbox
    def assigned(sandbox):
        record=json.loads(catalog()['environment:'+sandbox.id])
        assert record['result']['state']=='Running'
        return record['assignment']['node_id']
    def verify(name, expected, **options):
        print('[PLACEMENT RUN]',name,flush=True)
        sandbox=create(**options)
        actual=assigned(sandbox)
        assert actual==expected,(name,expected,actual)
        command=sandbox.commands.run('printf placement-ready')
        assert command.exit_code==0 and command.stdout=='placement-ready'
        sandbox.kill()
        report['cases'].append({'name':name,'expected_node':expected,'actual_node':actual,'instance_id':sandbox.id,'passed':True})
        print('[PLACEMENT PASS]',name,'node='+actual,flush=True)
    try:
        left=create(node_id='node1',labels={'peer':'left'})
        right=create(node_id='node2',labels={'peer':'right'})
        assert (assigned(left),assigned(right))==('node1','node2')
        verify('environment affinity OR','node1',schedule_affinities=[condition(1,2,'peer','missing'),condition(1,2,'peer','left')])
        verify('instance anti-affinity','node2',schedule_affinities=[condition(1,3,'peer','left')])
        # Equal anchor reservations keep the resource score equal across the two nodes.
        verify('weighted node preference','node2',schedule_affinities=[condition(0,0,'NODE_ID','node1',weight=1),condition(0,0,'NODE_ID','node2',weight=9)])
        verify('ordered node preference','node2',schedule_affinities=[condition(0,0,'NODE_ID','node2',preferredPriority=True),condition(0,0,'NODE_ID','node1',preferredPriority=True)])
        verify('node_id constrains every OR branch','node2',node_id='node2',schedule_affinities=[condition(0,2,'NODE_ID','node1'),condition(0,2,'NODE_ID','node2')])
        guard=create(node_id='node1',schedule_affinities=[condition(1,3,'future','yes')])
        assert assigned(guard)=='node1'
        # Prefer node1 to prove that the existing peer's required anti-affinity wins.
        verify('reverse instance anti-affinity','node2',labels={'future':'yes'},schedule_affinities=[condition(0,0,'NODE_ID','node1',weight=1000)])
        report['status']='passed'
    except Exception as error:
        report['error']=str(error)
        raise
    finally:
        for sandbox in reversed(handles):
            try:sandbox.kill()
            except Exception as error:report['cleanup_errors'].append(str(error))
            finally:sandbox.close()
        try:
            records=catalog()
            for sid in report['instances']:
                result=json.loads(records['environment:'+sid])['result']
                assert result['state']=='Deleted' and not result['resources_held'],sid
            report['terminal_resources_released']=True
        except Exception as error:report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:report['status']='failed'
        output.write_text(json.dumps(report,indent=2)+'\n')
        if report['cleanup_errors'] and 'error' not in report:raise RuntimeError('placement cleanup failed')
