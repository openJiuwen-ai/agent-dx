"""Default EROFS/OCI runtime and user-image bootstrap through the installed SDK."""
import json
from pathlib import Path
from adx_sandbox import Sandbox


def run(connection, image, output):
    results=[]
    for name, options in [('default', {}), ('runtime-only', {'runtime':'runc'}), ('custom', {'image':image})]:
        s=Sandbox(cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150,**options)
        try:
            command="test -x /__adx/usr/local/bin/rrt-runtime && printf 'environment-ready'"
            if name=='custom' and Path('/secrets/runtime-image').is_file():
                command="test ! -e /usr/local/bin/rrt-runtime && " + command
            result=s.commands.run(command)
            assert result.exit_code==0 and result.stdout=='environment-ready', (name,result)
            results.append({'mode':name,'id':s.id,'command':True})
            print('PASS runtime environment: '+name,flush=True)
        finally:
            try:s.kill()
            finally:s.close()
    output.write_text(json.dumps({'status':'passed','checks':results},indent=2))
