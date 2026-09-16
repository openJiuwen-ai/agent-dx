"""Acceptance for the installed, unmodified complete deployment example."""
CASES=('validate and render installed example','all five example roles ready with auto resources',
       'administrator creates tenant key through HTTPS Edge','SDK command and binary file round trip',
       'explicit instance deletion','supervisor stop deletes remaining instance')
def verify(result):
    if (result.get('status')!='passed' or not result.get('example_sha256')
            or result.get('example_sha256')!=result.get('installed_sha256')
            or result.get('backend_count')!=0
            or result.get('external_dependencies_alive_after_stop') is not True
            or result.get('cleanup_errors')!=[]
            or [c.get('name') for c in result.get('cases',[])]!=list(CASES)
            or not all(c.get('passed') is True for c in result['cases'])):
        raise ValueError('incomplete deployment example evidence')
