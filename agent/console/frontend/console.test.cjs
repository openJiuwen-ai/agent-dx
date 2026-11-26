const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=()=>fs.readFileSync(__dirname+'/console.js','utf8').replace(/^import .*;\n/gm,'');
test('terminal layout rules do not target the xterm internal terminal class',()=>{
  const css=fs.readFileSync(__dirname+'/console.css','utf8');
  assert.doesNotMatch(css,/\.terminal(?=[\s{,:])/);
  assert.match(css,/\.terminal-output/);
});
class Element {
  showModal(){this.open=true;}
  close(){this.open=false;this.onclose?.();}
  constructor(tag){this.tagName=tag;this.children=[];this.textContent='';}
  append(...children){for(const child of children){if(child instanceof Element)child.parent=this;this.children.push(child);}}
  replaceChildren(...children){this.children=[];this.append(...children);}
  remove(){if(this.parent)this.parent.children=this.parent.children.filter(child=>child!==this);}
  querySelectorAll(tag){return descendants(this).filter(child=>child.tagName===tag);}
  querySelector(tag){return this.querySelectorAll(tag)[0];}
}
function descendants(parent){return parent.children.flatMap(child=>child instanceof Element?[child,...descendants(child)]:[]);}
function harness(fetch){const nodes=new Map();const context=vm.createContext({Node:Element,URLSearchParams,AbortController,structuredClone,
  setTimeout(){return 1;},clearTimeout(){},crypto:{randomUUID:()=> 'seed-1'},
  location:{hash:'#/login'},history:{replaceState(){}},window:{addEventListener(){}},navigator:{},fetch,
  document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
});vm.runInContext(source(),context);return {context,app:nodes.get('#app'),run:code=>vm.runInContext(code,context)};}
const response=value=>({ok:true,status:200,json:async()=>value});
test('display name search submits the exact name with default dropdowns omitted',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);return response({users:[],total:0,degraded:[]});});
  await h.run("userList('wld')");
  descendants(h.app).find(n=>n.name==='display_name').value='mock-digitalmate-01';
  await descendants(h.app).find(n=>n.className==='tenant-filters user-filters').onsubmit({preventDefault(){}});
  assert.equal(calls.at(-1),'/api/console/v1/tenants/wld/users?display_name=mock-digitalmate-01');
});
test('user directory shows verified bindings as associated despite independent model degradation',async()=>{
  const h=harness(async path=>response(path.includes('/users?')?{users:[{user_id:'9b3074be-2fd9-49c3-a6af-9ea2bcad0b77',display_name:'mock-digitalmate-01',binding_state:'exists',has_binding:true,status:'active',key_state:'ready'}],total:1,degraded:['litellm']}:{modules:{business:true}}));
  await h.run("userList('wld')");
  const all=descendants(h.app);assert.ok(all.some(n=>n.textContent==='已关联'));assert.ok(!all.some(n=>n.textContent==='未关联'));
});
test('binding rows show identities and pin asynchronous status requests for both reference kinds',async()=>{
  for(const kind of ['versions','aliases']){
    const requests=[];const h=harness(async path=>{
      requests.push(path);
      if(path.includes('/environment/'))return response({environment:{status:path.includes('/secondary')?'paused':'running'}});
      if(path.includes('/bindings?'))return response({bindings:[{scope:{binding_id:'user'},generation:'7',phase:'active',current:{version:'3',environment_id:'env-a'},secondary:{environment:{version:'2',environment_id:'env-b'},intent:'replace'}}],total:1});
      if(path.includes('/versions?'))return response({versions:[{version:'3'}],total:1});
      if(path.includes('/aliases?'))return response({aliases:[{config:{alias:'alias1'}}],total:1});
      return response({modules:{},template:{},alias:{config:{versions:[]}}});
    });
    await h.run(`template('wld','digitalmate',new URLSearchParams('kind=${kind}&ref=${kind==='versions'?'3':'alias1'}'))`);
    await new Promise(resolve=>setImmediate(resolve));
    const text=descendants(h.app).map(n=>n.textContent);
    for(const value of ['Binding','阶段','当前环境 current','次要环境 secondary','generation · 7','active','env-a','env-b','replace','running','paused'])assert.ok(text.includes(value),value);
    assert.ok(descendants(h.app).some(n=>n.title==='env-a'));
    assert.ok(descendants(h.app).some(n=>n.textContent==='running'&&n.className.includes('is-running')));
    assert.equal(requests.filter(p=>p.includes('/environment/')).length,2);
    assert.ok(requests.some(p=>p.includes('/environment/current?expected_generation=7&environment_id=env-a')));
  }
});
test('binding state queue limits concurrency and discards superseded list responses',async()=>{
  const pending=[];let active=0,max=0;
  const h=harness(path=>{active++;max=Math.max(max,active);return new Promise(resolve=>pending.push(()=>{active--;resolve(response({environment:{status:'running'}}));}));});
  h.run("globalThis.owner=document.createElement('div');globalThis.sequence=1;globalThis.queue=bindingStateQueue(()=>sequence===1);globalThis.cells=[];for(let i=0;i<8;i++){const cell=document.createElement('span');cells.push(cell);queue('/state',cell);} ");
  assert.equal(max,4);assert.equal(pending.length,4);
  h.run('sequence=2');pending.splice(0).forEach(resolve=>resolve());await new Promise(resolve=>setImmediate(resolve));
  assert.equal(pending.length,0);assert.ok(h.run("cells.every(cell=>cell.textContent==='')"));
});
test('binding state queue distinguishes conflicts failures and confirmed absence',async()=>{
  const h=harness(async path=>path.endsWith('/absent')?response({environment:{status:'absent'}}):{ok:false,status:409,json:async()=>({error:{kind:path.endsWith('/changed')?'generation_conflict':'upstream_unavailable',message:'failed'}})});
  h.run("globalThis.cells=[document.createElement('span'),document.createElement('span'),document.createElement('span')];const queue=bindingStateQueue(()=>true);['/changed','/failed','/absent'].forEach((path,i)=>queue(path,cells[i]));");
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(h.run('cells[0].textContent'),'绑定已变化，请刷新');
  assert.equal(h.run('cells[1].textContent'),'未知');
  assert.equal(h.run('cells[2].textContent'),'不存在');
});
test('template directory displays latest configuration summary and explicit unavailable state',async()=>{
  const h=harness(async path=>response(path.includes('/templates?')?{templates:[{name:'jiuwen',latest_version:'1',configuration_summary:{cpu_millis:1000,memory_mib:512,service_count:2}},{name:'broken',latest_version:'3',configuration_summary:null}],total:2,next_page_token:null}:{modules:{}}));
  await h.run("templates('wld')");
  const all=descendants(h.app);
  assert.ok(all.some(n=>n.tagName==='th'&&n.textContent==='配置摘要'));
  assert.ok(all.some(n=>n.textContent==='jiuwen:1 · CPU 1000m · 内存 512 MiB · 服务 2 个'));
  assert.ok(all.some(n=>n.textContent==='摘要暂不可用'));
});
test('version detail alias tab links directly to the first alias detail',async()=>{
  const h=harness(async path=>{
    if(path.includes('/aliases?'))return response({aliases:[{config:{alias:'alias1'}}],total:1,next_page_token:null});
    if(path.includes('/versions?'))return response({versions:[{version:'3'}],total:1,next_page_token:null});
    if(path.includes('/bindings?'))return response({bindings:[],total:0,next_page_token:null});
    return response({modules:{},template:{name:'digitalmate',version:'3'}});
  });
  await h.run("template('wld','digitalmate',new URLSearchParams('kind=versions&ref=3'))");
  assert.equal(descendants(h.app).find(n=>n.tagName==='a'&&n.textContent==='别名').href,'#/tenants/wld/templates/digitalmate?kind=aliases&ref=alias1');
});
test('alias entry selects the first catalog alias and shows its binding and configuration',async()=>{
  const calls=[],replacements=[];
  const h=harness(async path=>{calls.push(path);if(path.includes('/aliases?'))return response({aliases:[{config:{alias:'alias1'}},{config:{alias:'stable'}}],total:2,next_page_token:null});if(path.endsWith('/aliases/alias1'))return response({alias:{config:{alias:'alias1',versions:[]}}});if(path.includes('/bindings?'))return response({bindings:[],total:0,next_page_token:null});return response({modules:{}});});
  h.context.history={replaceState:(_state,_title,url)=>replacements.push(url)};
  await h.run("template('wld','demo',new URLSearchParams('kind=aliases'))");
  assert.deepEqual(replacements,['#/tenants/wld/templates/demo?kind=aliases&ref=alias1']);
  assert.ok(calls.some(path=>path.endsWith('/aliases/alias1')));
  assert.ok(calls.some(path=>path.includes('/bindings?')&&path.includes('ref=alias1')));
  assert.ok(descendants(h.app).some(n=>n.textContent==='当前选择'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='Key / Value 配置'));
});
test('empty alias catalog retains creation without selecting an alias',async()=>{
  const h=harness(async path=>response(path.includes('/aliases?')?{aliases:[],total:0,next_page_token:null}:{modules:{}}));
  await h.run("template('wld','demo',new URLSearchParams('kind=aliases'))");
  assert.ok(descendants(h.app).some(n=>n.textContent==='创建别名'));
  assert.ok(!descendants(h.app).some(n=>n.textContent==='引用下 Binding'));
});
test('template entry selects the published latest version rather than the first catalog item',async()=>{
  const calls=[],replacements=[];
  const h=harness(async path=>{calls.push(path);if(path.endsWith('/templates/demo'))return response({template:{name:'demo',version:'12'}});if(path.includes('/versions?'))return response({versions:[{version:'1'},{version:'12'}],total:2,next_page_token:null});if(path.includes('/bindings?'))return response({bindings:[],total:0,next_page_token:null});return response({modules:{},template:{name:'demo',version:'12'}});});
  h.context.history={replaceState:(_state,_title,url)=>replacements.push(url)};
  await h.run("template('wld','demo',new URLSearchParams('kind=versions'))");
  assert.deepEqual(replacements,['#/tenants/wld/templates/demo?kind=versions&ref=12']);
  assert.ok(calls.some(path=>path.includes('/bindings?')&&path.includes('ref=12')));
  assert.ok(descendants(h.app).some(n=>n.textContent==='Key / Value 配置'));
  assert.ok(!calls.some(path=>path.endsWith('/versions/1')));
});
test('explicit template version is preserved without querying the default latest version',async()=>{
  const calls=[];
  const h=harness(async path=>{calls.push(path);if(path.includes('/versions?'))return response({versions:[{version:'1'}],total:1,next_page_token:null});if(path.includes('/bindings?'))return response({bindings:[],total:0,next_page_token:null});return response({modules:{},template:{name:'demo',version:'1'}});});
  await h.run("template('wld','demo',new URLSearchParams('kind=versions&ref=1'))");
  assert.ok(!calls.some(path=>path.endsWith('/templates/demo')));
  assert.ok(calls.some(path=>path.endsWith('/versions/1')));
});
test('user directory translates persisted model credential readiness',async()=>{
  const h=harness(async()=>response({users:['ready','pending','absent'].map((key_state,i)=>({user_id:'u'+i,key_state})),total:3,next_page_token:null}));h.app.replaceChildren();await h.run("userList('wld')");
  for(const label of ['已就绪','待就绪','未配置'])assert.ok(descendants(h.app).some(n=>n.textContent===label),label);
});
test('user quota query time renders a calendar date instead of a raw timestamp',async()=>{
  for(const queried_at of [1795079466,'2026-11-19T09:11:06Z']){
    const data=userDetailFixture();data.quota.queried_at=queried_at;
    const h=harness(async()=>response(data));h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
    const label=descendants(h.app).find(n=>n.tagName==='dt'&&n.textContent==='查询时间'),value=label.parent.children[label.parent.children.indexOf(label)+1];
    assert.match(value.textContent,/2026年11月19日/);assert.notEqual(value.textContent,String(queried_at));
  }
});
const userDetailFixture=()=>({user:{user_id:'user-1',display_name:'测试用户',provider_label:'AGC',status:'active',created_at:'2026-11-19T03:08:11Z',identity_scope:{developer_scope:'project',client_id:'client'}},litellm:{user_id:'model-owner',key_state:'exists',key:{key_id:'key-1',key_display:'sk-…1234',config_status:'ready',recoverable:true}},quota:{total:'30',used:'0',balance:null,field_states:{total:'known',used:'known',balance:'failed'},unit:'points'},agent_associations:[],degraded:[]});
test('quota refresh prevents duplicates, updates its card and retains data on failure',async()=>{
  const h=harness(async()=>response(userDetailFixture()));h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
  const refresh=descendants(h.app).find(n=>n.textContent==='刷新额度');assert.ok(refresh);
  let resolve,calls=0;h.context.fetch=()=>{calls++;return new Promise(done=>{resolve=done;});};
  const pending=refresh.onclick();assert.equal(refresh.disabled,true);await refresh.onclick();assert.equal(calls,1);
  const next=userDetailFixture();next.quota.used='5';next.quota.balance='25';next.quota.field_states.balance='known';resolve(response(next));await pending;
  const metrics=()=>descendants(h.app).filter(n=>n.className==='user-metric').map(n=>descendants(n).find(c=>c.tagName==='strong').textContent);
  assert.deepEqual(metrics(),['30','5','25']);assert.equal(refresh.disabled,false);
  h.context.fetch=async()=>{throw new Error('network failed');};await refresh.onclick();assert.deepEqual(metrics(),['30','5','25']);assert.equal(refresh.disabled,false);
  assert.ok(descendants(h.app).some(n=>String(n.textContent).includes('刷新失败')));
});
test('user detail presents workspace cards, Chinese labels and unknown quota without zero coercion',async()=>{
  const h=harness(async()=>response(userDetailFixture()));h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
  assert.ok(h.app.className.includes('tenant-page'));
  assert.ok(descendants(h.app).some(n=>n.tagName==='h1'&&n.textContent==='测试用户'));
  assert.ok(descendants(h.app).some(n=>n.href==='#/tenants/wld/users'));
  assert.deepEqual(descendants(h.app).filter(n=>n.tagName==='h2').map(n=>n.textContent),['基本信息','额度概览','模型 Key','Agent 关联']);
  assert.ok(descendants(h.app).some(n=>n.textContent==='用户 ID'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='暂无 Agent 关联'));
  const metrics=descendants(h.app).filter(n=>n.className==='user-metric');
  assert.deepEqual(metrics.map(n=>descendants(n).find(c=>c.tagName==='strong').textContent),['30','0','查询失败']);
});
test('user key reveal replaces prior plaintext and clears it after fifteen seconds',async()=>{
  const calls=[],timers=[];let copied;
  const h=harness(async(path,options)=>{if(path.endsWith('/reveal')){calls.push(JSON.parse(options.body));return response({key:'sk-private'});}return response(userDetailFixture());});
  h.context.setTimeout=(fn,ms)=>{timers.push({fn,ms});return timers.length;};h.context.navigator.clipboard={writeText:async value=>{copied=value;}};
  h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
  const view=descendants(h.app).find(n=>n.textContent==='查看明文');await view.onclick();await view.onclick();
  assert.equal(descendants(h.app).filter(n=>n.tagName==='input'&&n.value==='sk-private').length,1);
  assert.equal(timers.at(-1).ms,15000);timers.at(-1).fn();
  assert.equal(descendants(h.app).filter(n=>n.tagName==='input').length,0);
  await descendants(h.app).find(n=>n.textContent==='复制 Key').onclick();assert.equal(copied,'sk-private');assert.equal(calls.at(-1).purpose,'copy');
});
test('unrecoverable user keys stay disabled and degraded associations remain explicit',async()=>{
  const data=userDetailFixture();data.litellm.key.recoverable=false;data.degraded=['litellm'];data.agent_associations=[{kind:'digitalmate',template:'digitalmate',ref:{kind:'version',name:'3'},binding_id:'binding-long-id',existence:'uncertain'}];
  const h=harness(async()=>response(data));await h.run("userDetail('wld','user-1')");
  assert.equal(descendants(h.app).find(n=>n.textContent==='查看明文').disabled,true);
  assert.ok(descendants(h.app).some(n=>n.textContent==='关联暂无法确认'));
  assert.ok(descendants(h.app).some(n=>String(n.textContent).includes('部分数据暂不可用')));
});
test('user associations distinguish expected identities from existing bindings',async()=>{
   const data=userDetailFixture();data.agent_associations=['not_created','exists','uncertain'].map((existence,i)=>({kind:'digitalmate',template:'digitalmate',refs:existence==='exists'?[{kind:'alias',name:'production'}]:[],binding_id:'binding-'+i,existence}));
  const h=harness(async()=>response(data));h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
  const all=descendants(h.app);for(const text of ['尚未创建','已存在','暂无法确认','预期 Binding ID'])assert.ok(all.some(n=>n.textContent===text),text);
  assert.ok(all.some(n=>n.href==='#/tenants/wld/templates/digitalmate?kind=aliases&ref=production'));
   assert.equal(all.filter(n=>n.textContent==='查看 Binding →').length,0);
   assert.ok(all.some(n=>n.tagName==='span'&&n.textContent==='别名：'));
   assert.ok(all.some(n=>n.tagName==='a'&&n.textContent==='production'));
});
test('absent associations hide references and existing or partial associations list every verified reference',async()=>{
  const data=userDetailFixture();data.agent_associations=[
    {kind:'jiuwen',template:'jiuwen',binding_id:'absent',existence:'not_created',ref:{kind:'alias',name:'must-hide'},refs:[]},
    {kind:'orbit',template:'orbit',binding_id:'existing',existence:'exists',refs:[{kind:'alias',name:'stable'},{kind:'version',name:'2'}]},
    {kind:'digitalmate',template:'digitalmate',binding_id:'partial',existence:'uncertain',refs:[{kind:'version',name:'3'}]}
  ];
  const h=harness(async()=>response(data));h.app.replaceChildren();await h.run("userDetail('wld','user-1')");
  const all=descendants(h.app),links=all.filter(n=>n.tagName==='a');
  assert.ok(!links.some(n=>n.href.includes('must-hide')));
   for(const target of ['/orbit?kind=aliases&ref=stable','/orbit?kind=versions&ref=2','/digitalmate?kind=versions&ref=3'])assert.ok(links.some(n=>n.href.endsWith(target)),target);
  assert.ok(all.some(n=>n.textContent==='暂无法确认'));
});
test('successful binding deletion returns to the template detail',async()=>{
  let deleted=false;
  const h=harness(async(path,options)=>{
    if(options?.method==='DELETE'){deleted=true;return response({status:'deleted'});}
    return response({binding:{scope:{tenant:'wld',template:'digitalmate',binding_id:'demo-v2'},generation:'7'}});
  });
  h.context.prompt=()=> 'demo-v2';h.context.location.hash='#/tenants/wld/bindings/digitalmate/versions/3/demo-v2';
  await h.run("binding('wld','digitalmate','versions','3','demo-v2')");
  await descendants(h.app).find(n=>n.textContent==='强制删除').onclick();
  assert.equal(deleted,true);assert.equal(h.context.location.hash,'/tenants/wld/templates/digitalmate?kind=versions&ref=3');
});
test('tenant administrators have no breadcrumb row while platform administrators retain navigation',async()=>{
  for(const role of ['tenant_admin','platform_admin']){
    const h=harness(async()=>response({role,subject_id:'admin',landing_path:'/tenants/wld/templates'}));
    h.context.location.hash='#/tenants/wld/templates';
    await h.run('templates=async()=>{};route()');
    assert.equal(h.context.document.querySelector('#nav').hidden,role==='tenant_admin');
  }
});
test('binding figure 11 layout separates environment cards, files and danger actions',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);
    if(path.includes('/environment/'))return {ok:false,status:503,json:async()=>({error:{kind:'upstream_unavailable',message:'查询失败'}})};
    if(path.includes('/files/mounts'))return response({mounts:[]});
    return response({binding:{scope:{tenant:'wld',template:'digitalmate',template_ref:{kind:'version',name:'2'},binding_id:'demo-v2'},phase:'active',generation:'7',current:{environment_id:'env-current',version:'2'}}});
  });h.app.replaceChildren();await h.run("binding('wld','digitalmate','versions','2','demo-v2')");
  assert.deepEqual(descendants(h.app).filter(n=>n.tagName==='h2').map(n=>n.textContent),['Binding 摘要','环境','文件（只读）','危险操作']);
  const cards=descendants(h.app).filter(n=>n.className==='binding-env-card');assert.equal(cards.length,2);
  assert.ok(descendants(cards[0]).some(n=>n.textContent==='unknown'));
  assert.ok(descendants(cards[1]).some(n=>n.textContent==='未配置次要环境'));
  assert.ok(descendants(cards[0]).find(n=>n.textContent==='连接终端').disabled);
  const before=calls.length;await descendants(cards[0]).find(n=>n.textContent==='重新查询').onclick();assert.equal(calls.length,before+1);
});
test('binding detail uses workspace cards and two-column identity fields',async()=>{
  const h=harness(async()=>response({binding:{scope:{tenant:'wld',template:'digitalmate',binding_id:'demo-v2'},generation:'7'}}));
  h.app.replaceChildren();await h.run("binding('wld','digitalmate','versions','2','demo-v2')");
  assert.ok(h.app.className.includes('tenant-page'));
  assert.ok(descendants(h.app).some(n=>n.tagName==='h1'&&n.textContent==='demo-v2'));
  assert.ok(descendants(h.app).some(n=>n.tagName==='table'));
  assert.ok(descendants(h.app).filter(n=>n.tagName==='section').every(n=>n.className.includes('tenant-panel')));
});
test('alias creation and weight editing use one modal with cancel controls',()=>{
  const h=harness();h.app.replaceChildren();h.run("aliasEditor('t','app')");
  let dialogs=descendants(h.app).filter(n=>n.tagName==='dialog');assert.equal(dialogs.length,1);assert.equal(dialogs[0].open,true);
  assert.ok(descendants(dialogs[0]).some(n=>n.textContent==='创建别名'));
  h.run("aliasEditor('t','app',{revision:'r1',config:{alias:'stable',seed:'seed',versions:[{version:'1',weight:100}]}})");
  dialogs=descendants(h.app).filter(n=>n.tagName==='dialog');assert.equal(dialogs.length,1);
  assert.equal(descendants(dialogs[0]).find(n=>n.placeholder==='别名').readOnly,true);
  descendants(dialogs[0]).find(n=>n.textContent==='取消').onclick();
  assert.equal(descendants(h.app).filter(n=>n.tagName==='dialog').length,0);
});
test('version editor opens a single dismissible modal instead of accumulating sections',()=>{
  const h=harness();h.app.replaceChildren();
  h.run("publishEditor('t','app',{name:'app',version:'1',image:'old'})");
  let dialogs=descendants(h.app).filter(n=>n.tagName==='dialog');assert.equal(dialogs.length,1);assert.equal(dialogs[0].open,true);
  h.run("publishEditor('t','app',{name:'app',version:'1',image:'old'})");
  dialogs=descendants(h.app).filter(n=>n.tagName==='dialog');assert.equal(dialogs.length,1);
  descendants(dialogs[0]).find(n=>n.textContent==='取消').onclick();
  assert.equal(descendants(h.app).filter(n=>n.tagName==='dialog').length,0);
});
test('template configuration renders nested objects and arrays as flat key value rows',()=>{
  const h=harness();h.app.replaceChildren();
  h.run("configurationFields({template:{resources:{cpu_millis:1000},service:[{port:8080}],env:{},mounts:[],optional:null}},app)");
  const tables=descendants(h.app).filter(n=>n.tagName==='table');assert.equal(tables.length,1);
  const rows=descendants(tables[0]).filter(n=>n.tagName==='tr');
  assert.ok(rows.every(row=>row.children.length===2));
  assert.deepEqual(rows.slice(1).map(row=>row.children.map(cell=>cell.children[0])),[
    ['template.resources.cpu_millis','1000'],['template.service[0].port','8080'],
    ['template.env','{}'],['template.mounts','[]'],['template.optional','未知'],
  ]);
});
test('template detail identifies the tenant and template in the browser tab and workspace heading',async()=>{
  const h=harness(async()=>response({versions:[{version:'1'}],total:1,next_page_token:null}));
  h.app.replaceChildren();await h.run("template('wld','digitalmate',new URLSearchParams())");
  assert.equal(h.run('document.title'),'/wld/模板digitalmate');
  assert.ok(h.app.className.includes('tenant-page'));
  assert.ok(descendants(h.app).some(n=>n.tagName==='h1'&&n.textContent==='digitalmate'));
  assert.ok(descendants(h.app).some(n=>n.href==='#/tenants/wld/templates'&&n.textContent.includes('返回')));
});
test('business query failures stay in one local status and clear after recovery',async()=>{
  let available=false;
  const h=harness(async path=>{
    if(path.includes('/users?'))return available?response({users:[],total:0,next_page_token:null}):{ok:false,status:503,json:async()=>({error:{kind:'accounts_unavailable',message:'业务用户查询失败，请重试'}})};
    return response(path.includes('/templates?')?{templates:[],total:0,next_page_token:null}:{display_name:'Team',modules:{business:true}});
  });
  h.app.replaceChildren();await h.run("userList('t')");
  const area=descendants(h.app).find(n=>n.tagName==='h2'&&n.textContent==='业务用户').parent;
  const errors=()=>descendants(h.app).filter(n=>n.textContent.includes('accounts_unavailable'));
  assert.equal(errors().length,1);assert.ok(descendants(area).includes(errors()[0]));
  const form=descendants(area).find(n=>n.tagName==='form');
  for(let i=0;i<3;i++)await form.onsubmit({preventDefault(){}});
  assert.equal(errors().length,1);
  available=true;await form.onsubmit({preventDefault(){}});
  assert.equal(errors().length,0);assert.equal(descendants(area).filter(n=>n.tagName==='table').length,1);
});
test('business query blocks repeated submissions until the request settles',async()=>{
  const h=harness(async()=>response({users:[],total:0,next_page_token:null}));
  h.app.replaceChildren();await h.run("userList('t')");
  let resolve,calls=0;h.context.fetch=()=>{calls++;return new Promise(done=>{resolve=done;});};
  const form=descendants(h.app).find(n=>n.tagName==='form'),submit=descendants(form).find(n=>n.type==='submit');
  const pending=form.onsubmit({preventDefault(){}});
  assert.equal(submit.disabled,true);await form.onsubmit({preventDefault(){}});assert.equal(calls,1);
  resolve(response({users:[],total:0,next_page_token:null}));await pending;
  assert.equal(submit.disabled,false);
});
test('template workspace shows tenant context and supports filtering and empty state',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);return response(path.includes('/templates?')?{templates:[],total:0,next_page_token:null}:{tenant:{display_name:'Example Team',type:'standard'},modules:{business:false}});});
  h.app.replaceChildren();await h.run("templates('example')");
  assert.ok(h.app.className.includes('tenant-page'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='模板工作空间'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='当前没有匹配的模板'));
  const input=descendants(h.app).find(n=>n.name==='name');input.value='orbit';
  await descendants(h.app).find(n=>n.tagName==='form').onsubmit({preventDefault(){}});
  assert.ok(calls.at(-1).includes('name=orbit'));
});
test('unknown and malformed routes replace history according to authentication',async()=>{
  for(const hash of ['#???','##/tenants/wld/templates','#/wrong','#/tenants/wld/templates/app/extra','#/tenants/%ZZ/users'])for(const loggedIn of [true,false]){
    const h=harness(async()=>loggedIn?response({role:'tenant_admin',subject_id:'wld',landing_path:'#/tenants/wld/templates'}):{ok:false,status:401,json:async()=>({error:{kind:'unauthorized'}})});
    h.context.location.hash=hash;let replaced;h.context.location.replace=value=>{replaced=value;};
    await h.run('route()');assert.equal(replaced,loggedIn?'#/tenants/wld/templates':'#/login');
  }
});
test('platform unknown route uses the server hash landing path without duplicating the hash',async()=>{
  const h=harness(async()=>response({role:'platform_admin',subject_id:'admin',landing_path:'#/tenants'}));h.context.location.hash='#???';let replaced;h.context.location.replace=value=>{replaced=value;};await h.run('route()');assert.equal(replaced,'#/tenants');
});
test('workspace tabs separate templates and users and reflect business capability',async()=>{
  for(const business of [true,false]){
    const calls=[];const h=harness(async path=>{calls.push(path);return response(path.includes('/templates?')?{templates:[],total:0,next_page_token:null}:{display_name:'Team',modules:{business}});});
    h.app.replaceChildren();await h.run("templates('wld')");
    assert.equal(calls.some(path=>path.includes('/users')),false);
    assert.equal(descendants(h.app).some(n=>n.href==='#/tenants/wld/users'),business);
  }
  const h=harness(async path=>response(path.includes('/users?')?{users:[],total:0,next_page_token:null}:{modules:{business:true}}));
  h.app.replaceChildren();await h.run("userWorkspace('wld')");
  assert.ok(h.app.className.includes('tenant-page'));
  assert.ok(descendants(h.app).some(n=>n.tagName==='h1'&&n.textContent==='用户管理台'));
  assert.ok(descendants(h.app).some(n=>n.href==='#/tenants/wld/users'&&n.className.includes('is-active')));
  assert.ok(descendants(h.app).some(n=>n.textContent==='查询用户'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='重置'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='当前没有匹配的用户'));
});
test('tenant directory filters and resets with styled workspace entries',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);return response({tenants:[{tenant_id:'t1',display_name:'Workspace One',type:'standard'}],total:1,next_page_token:null});});
  h.app.replaceChildren();await h.run('tenants()');assert.equal(h.app.className,'tenant-page');
  const all=()=>descendants(h.app),form=all().find(n=>n.tagName==='form');
  assert.ok(all().some(n=>n.textContent==='租户工作空间'));
  assert.ok(all().some(n=>n.href?.split('?')[0]==='#/tenants/t1/templates'));
  all().find(n=>n.name==='name').value='One';await form.onsubmit({preventDefault(){}});
  assert.ok(calls.at(-1).includes('name=One'));
  await all().find(n=>n.textContent==='重置').onclick();assert.equal(calls.at(-1),'/api/console/v1/tenants?');
});
test('reference pages paginate server filters and restore binding return context',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);const url=new URL(path,'https://console');
    if(url.pathname.endsWith('/versions'))return response({versions:[{version:url.searchParams.has('page_token')?'1':'2'}],total:2,next_page_token:url.searchParams.has('page_token')?null:'cursor-2'});
    if(url.pathname.endsWith('/aliases'))return response({aliases:[],total:0,next_page_token:null});
    if(url.pathname.endsWith('/bindings'))return response({bindings:[{scope:{binding_id:'full-id'}}],total:1,next_page_token:null});
    return response({template:{name:'app',version:'2',image:'image'}});
  });
  await h.run("template('t','app',new URLSearchParams('kind=versions&ref=2&version=2&page_size=1'))");
  const next=descendants(h.app).find(n=>n.tagName==='button'&&n.textContent==='下一页');assert.ok(next);
  await next.onclick();
  assert.ok(calls.some(path=>path.includes('/versions?')&&path.includes('page_token=cursor-2')&&path.includes('version=2')));
  const detail=descendants(h.app).find(n=>n.tagName==='a'&&n.textContent==='查看详情 →'&&n.href.includes('/bindings/'));assert.ok(detail.href.includes('return='));
  const back=new URLSearchParams(detail.href.split('?')[1]).get('return');assert.ok(back.includes('kind=versions'));assert.ok(back.includes('ref=2'));assert.ok(back.includes('version=2'));
});
test('configuration editor can add and remove array rows and dictionary entries',()=>{
  const h=harness();h.context.parent=new Element('section');
  h.run("globalThis.read=editValues({env:{OLD:'x'},service:[]},parent)");
  const all=()=>descendants(h.context.parent);
  const add=all().find(n=>n.tagName==='button'&&n.textContent==='添加条目');assert.ok(add,'dictionary add action');
  const key=all().find(n=>n.placeholder==='新键');const value=all().find(n=>n.placeholder==='新值');key.value='NEW';value.value='y';add.onclick();
  assert.equal(h.context.read().env.NEW,'y');
  const row=all().find(n=>n.tagName==='button'&&n.textContent==='添加行');assert.ok(row,'array add action');row.onclick();assert.equal(h.context.read().service.length,1);
  const remove=all().find(n=>n.tagName==='button'&&n.textContent==='删除 OLD');remove.onclick();assert.equal('OLD'in h.context.read().env,false);
});
test('publishing selected version preserves masked fields and navigates to allocated version',async()=>{
  let submitted;const h=harness(async(path,options)=>{submitted=JSON.parse(options.body);return response({template:{version:'3'}});});
  h.run("publishEditor('t','app',{name:'app',version:'2',image:'old',env:{API_KEY:'********'},resources:{cpu_millis:1000,memory_mib:128}})");
  const all=descendants(h.app);all.find(n=>n.tagName==='input'&&n.value==='old').value='new';
  await all.find(n=>n.tagName==='button'&&n.textContent==='发布不可变版本').onclick();
  assert.equal(submitted.source_version,'2');assert.equal(submitted.template.image,'new');assert.equal(submitted.template.env.API_KEY,'********');assert.equal('version'in submitted.template,false);
  assert.equal(h.context.location.hash,'/tenants/t/templates/app?kind=versions&ref=3');
});
test('alias save carries original revision and seed while preserving multiple weights',async()=>{
  let submitted;const h=harness(async(path,options)=>{submitted=JSON.parse(options.body);return response({});});
  h.run("aliasEditor('t','app',{revision:'r1',config:{alias:'stable',seed:'immutable',versions:[{version:'1',weight:25},{version:'2',weight:75}]}})");
  const all=descendants(h.app);assert.equal(all.find(n=>n.tagName==='input'&&n.value==='stable').readOnly,true);
  await all.find(n=>n.tagName==='button'&&n.textContent==='保存权重').onclick();
  assert.equal(submitted.expected_revision,'r1');assert.equal(submitted.seed,'immutable');assert.deepEqual(submitted.versions,[{version:'1',weight:25},{version:'2',weight:75}]);
});
test('reference configuration follows bindings and creation action follows selected tab',async()=>{
  const h=harness(async path=>response(path.includes('/bindings?')?{bindings:[],total:0}:path.includes('/versions?')?{versions:[{version:'1'}],total:1}:{template:{name:'app',version:'1'}}));
  await h.run("template('t','app',new URLSearchParams('kind=versions&ref=1'))");
  const titles=descendants(h.app).filter(n=>n.tagName==='h2').map(n=>n.textContent);
  assert.ok(titles.indexOf('引用下 Binding')<titles.indexOf('Key / Value 配置'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='创建版本'));assert.equal(descendants(h.app).some(n=>n.textContent==='创建别名'),false);
});
test('binding back navigation restores only same tenant template workspace',async()=>{
  const h=harness(async()=>response({binding:{scope:{tenant:'t'},generation:'7'}}));
  h.context.location.hash='#/tenants/t/bindings/app/versions/1/id?return='+encodeURIComponent('/tenants/t/templates/app?kind=versions&ref=1&binding_id=id');
  await h.run("binding('t','app','versions','1','id')");
  const back=descendants(h.app).find(n=>n.tagName==='a'&&n.textContent==='返回模板详情');assert.ok(back);assert.equal(back.href,'#/tenants/t/templates/app?kind=versions&ref=1&binding_id=id');
});
test('alias name filter is translated to gateway name query and keeps literal characters',async()=>{
  let requested;const h=harness(async path=>{requested=path;return response({aliases:[],total:0});});
  await h.run("api('/tenants/t/templates/app/aliases?alias=prod%25_&page_size=1')");
  const query=new URL(requested,'https://console').searchParams;assert.equal(query.get('name'),'prod%_');assert.equal(query.has('alias'),false);
});
test('previous page reuses exact original filters without next cursor',async()=>{
  const calls=[];const h=harness(async path=>{calls.push(path);return response({templates:[],total:2,next_page_token:path.includes('page_token')?null:'next'});});
  h.context.parent=new Element('section');await h.run("list(parent,'/tenants/t/templates','templates',[],()=>[],new URLSearchParams('name=app&page_size=1'))");
  await descendants(h.context.parent).find(n=>n.textContent==='下一页').onclick();
  await descendants(h.context.parent).find(n=>n.textContent==='上一页').onclick();
  assert.equal(calls[0],calls[2]);
});
test('same-page newer filter replaces older delayed results',async()=>{
  const pending=[];const h=harness(path=>new Promise(resolve=>pending.push({path,resolve})));
  h.context.parent=new Element('section');
  const old=h.run("list(parent,'/tenants/t/templates','templates',['模板'],v=>[v.name],new URLSearchParams('name=old'))");
  const fresh=h.run("list(parent,'/tenants/t/templates','templates',['模板'],v=>[v.name],new URLSearchParams('name=new'))");
  pending[1].resolve(response({templates:[{name:'new-result'}],total:1}));await fresh;
  pending[0].resolve(response({templates:[{name:'old-result'}],total:1}));await old;
  const text=JSON.stringify(h.context.parent,(key,value)=>key==='parent'?undefined:value);assert.ok(text.includes('new-result'));assert.equal(text.includes('old-result'),false);
});
test('binding second-page detail return contains current binding cursor',async()=>{
  const h=harness(async path=>response({bindings:[{scope:{binding_id:'id'}}],total:2,next_page_token:path.includes('page_token')?null:'b2'}));h.context.parent=new Element('section');
  await h.run("list(parent,'/tenants/t/bindings','bindings',['ID'],v=>[link('详情','/tenants/t/bindings/app/versions/1/id?return='+enc('/tenants/t/templates/app?kind=versions&ref=1'))],new URLSearchParams('template=app&ref_kind=versions&ref=1'))");
  await descendants(h.context.parent).find(n=>n.textContent==='下一页').onclick();
  const anchor=descendants(h.context.parent).find(n=>n.tagName==='a');const back=new URLSearchParams(anchor.href.split('?')[1]).get('return');assert.equal(new URLSearchParams(back.split('?')[1]).get('binding_page_token'),'b2');
});
test('binding deletion requires typing exact full binding identity',()=>{
  const nodes=new Map();let answer='wrong-id';
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    prompt:()=>answer,location:{hash:'#/login'},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
  });
  vm.runInContext(source(),context);
  assert.equal(vm.runInContext("confirmBindingDelete('full-binding-id')",context),false);
  answer='full-binding-id';
  assert.equal(vm.runInContext("confirmBindingDelete('full-binding-id')",context),true);
});
test('late response from previous route cannot resolve into current view',async()=>{
  const nodes=new Map();let respond;
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    location:{hash:'#/login'},history:{replaceState(){}},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
    fetch:()=>new Promise(resolve=>{respond=()=>resolve({ok:true,status:200,json:async()=>({private:'old-tenant'})});}),
  });
  vm.runInContext(source(),context);
  const pending=vm.runInContext("api('/tenants/old')",context);
  vm.runInContext('epoch++',context);
  respond();
  await assert.rejects(pending,error=>error.name==='AbortError');
});
test('typed configuration editor retains boolean and numeric values',()=>{
  const nodes=new Map();
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    location:{hash:'#/login'},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
  });
  vm.runInContext(source(),context);
  context.parent=new Element('section');
  vm.runInContext("globalThis.readDraft=editValues({image:'old',resources:{cpu:2},enabled:true},parent)",context);
  const inputs=[];const visit=n=>{if(n instanceof Element){if(n.tagName==='input')inputs.push(n);n.children.forEach(visit);}};visit(context.parent);
  inputs.find(n=>n.type==='number').value='4';
  inputs.find(n=>n.type==='checkbox').checked=false;
  const result=context.readDraft();
  assert.equal(result.resources.cpu,4);
  assert.equal(result.enabled,false);
  assert.equal(result.image,'old');
});
test('session expiry clears sensitive content and closes terminal',()=>{
  const nodes=new Map();let expiry,closed=false;
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,
    setTimeout(callback){expiry=callback;return 1;},clearTimeout(){},
    location:{hash:'#/login'},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
  });
  vm.runInContext(source(),context);
  context.closeTerminal=()=>{closed=true;};
  vm.runInContext("app.append(node('input',undefined,{value:'sensitive'}));terminal={close:closeTerminal};setSession({expires_at:Math.floor(Date.now()/1000)+1})",context);
  expiry();
  assert.equal(closed,true);
  assert.ok(descendants(nodes.get('#app')).some(n=>n.tagName==='form'));
  assert.ok(descendants(nodes.get('#app')).some(n=>n.textContent==='登录已过期，请重新登录。'));
  assert.equal(context.location.hash,'/login');
});
test('invalid login preserves form and account with a helpful message on same hash',async()=>{
  const h=harness(async()=>({ok:false,status:401,json:async()=>({error:{kind:'invalid_credentials',message:'invalid credentials'}})}));
  const form=descendants(h.app).find(n=>n.tagName==='form');
  const inputs=descendants(form).filter(n=>n.tagName==='input');inputs[0].value='operator';inputs[1].value='wrong';
  await form.onsubmit({preventDefault(){}});
  assert.ok(descendants(h.app).includes(form));assert.equal(inputs[0].value,'operator');assert.equal(inputs[1].value,'');
  assert.ok(descendants(h.app).some(n=>n.textContent.includes('账号或管理凭据不正确')));
  assert.equal(descendants(form).find(n=>n.type==='submit').disabled,false);
});
test('logout immediately renders login and success notice even on unchanged hash',async()=>{
  const h=harness(async()=>({ok:true,status:204}));
  await h.run("document.querySelector('#logout').onclick()");
  assert.ok(descendants(h.app).some(n=>n.tagName==='form'));
  assert.ok(descendants(h.app).some(n=>n.textContent==='已安全退出，请重新登录。'));
});
test('login prevents duplicate submissions and recovers from network failure',async()=>{
  let reject,calls=0;const h=harness(()=>{calls++;return new Promise((_,fail)=>{reject=fail;});});
  const form=descendants(h.app).find(n=>n.tagName==='form'),submit=descendants(form).find(n=>n.type==='submit');
  for(const input of descendants(form).filter(n=>n.tagName==='input'))input.value='test';
  const pending=form.onsubmit({preventDefault(){}});assert.equal(submit.disabled,true);
  await form.onsubmit({preventDefault(){}});assert.equal(calls,1);
  reject(new TypeError('network unavailable'));await pending;
  assert.equal(submit.disabled,false);assert.ok(descendants(h.app).some(n=>n.textContent.includes('请检查网络')));
});
test('alias catalog links use the real nested AliasSnapshot config',async()=>{
  const nodes=new Map();
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    location:{hash:'#/login'},history:{replaceState(){}},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
    fetch:async url=>response(url.endsWith('/aliases/production')?{alias:{config:{alias:'production',versions:[]},revision:'r1'}}:url.includes('/bindings?')?{bindings:[],total:0,next_page_token:null}:{aliases:[{config:{alias:'production',versions:[]},revision:'r1'}],total:1,next_page_token:null}),
  });
  vm.runInContext(source(),context);
  await vm.runInContext("template('t','app',new URLSearchParams('kind=aliases'))",context);
  const all=[];const visit=node=>{if(node instanceof Element){all.push(node);node.children.forEach(visit);}};visit(nodes.get('#app'));
  assert.ok(all.some(node=>node.tagName==='a'&&node.href.endsWith('ref=production')));
});
function terminalHarness(){
  const h=harness(),sockets=[],displays=[];
  h.context.Terminal=class {constructor(){displays.push(this);this.output='';this.cols=80;this.rows=24;}open(){}loadAddon(){}onData(fn){this.input=fn;}onResize(fn){this.resize=fn;}write(data){this.output+=data;}dispose(){this.disposed=true;}focus(){}};
  h.context.FitAddon=class {fit(){displays.at(-1).fits=(displays.at(-1).fits||0)+1;}};
  h.context.WebSocket=class {constructor(url){this.url=url;this.readyState=1;this.sent=[];sockets.push(this);}send(value){this.sent.push(JSON.parse(value));}close(){this.readyState=3;this.onclose?.({code:1000});}};
  h.context.parent=new Element('div');h.context.location.protocol='https:';h.context.location.host='localhost:28443';
  return {...h,sockets,displays,open:()=>h.run("openTerminal(parent,'/terminal/current?expected_generation=7&environment_id=env-a')")};
}
test('terminal repeated connect keeps one panel and close removes it',()=>{
   const h=terminalHarness();h.open();h.open();assert.equal(h.sockets.length,1);assert.equal(descendants(h.context.parent).filter(n=>n.className==='terminal-output').length,1);
  descendants(h.context.parent).find(n=>n.textContent==='关闭终端').onclick();assert.equal(h.context.parent.children.length,0);assert.equal(h.displays[0].disposed,true);
});
test('terminal reports connection failure and resize sends dimensions even when unchanged',()=>{
  const h=terminalHarness();h.open();const ws=h.sockets[0];ws.onmessage({data:JSON.stringify({type:'ready'})});ws.onmessage({data:JSON.stringify({type:'output',data:'shell prompt'})});assert.match(h.displays[0].output,/shell prompt/);
  const before=ws.sent.length;descendants(h.context.parent).find(n=>n.textContent==='适配终端尺寸').onclick();assert.equal(ws.sent.length,before+1);assert.equal(ws.sent.at(-1).type,'resize');
  ws.onerror();assert.ok(descendants(h.context.parent).some(n=>n.textContent.includes('连接失败')));
   h.open();assert.equal(descendants(h.context.parent).filter(n=>n.className==='terminal-output').length,1);assert.equal(h.sockets.length,2);
  ws.onclose({code:1006});assert.equal(h.displays[1].disposed,undefined);
});
test('terminal size control toggles fullscreen and restores the existing session',()=>{
  const h=terminalHarness();h.open();const ws=h.sockets[0];ws.onmessage({data:JSON.stringify({type:'ready'})});
  const panel=h.context.parent.children[0],size=descendants(panel).find(n=>n.textContent==='适配终端尺寸');
  size.onclick();assert.equal(panel.className,'terminal-panel is-fullscreen');assert.equal(size.textContent,'取消适配终端尺寸');assert.equal(ws.sent.at(-1).type,'resize');
  size.onclick();assert.equal(panel.className,'terminal-panel');assert.equal(size.textContent,'适配终端尺寸');assert.equal(h.sockets.length,1);assert.equal(h.displays[0].disposed,undefined);
  size.onclick();ws.onerror();assert.equal(panel.className,'terminal-panel');assert.equal(size.textContent,'适配终端尺寸');
});
test('terminal websocket query uses fixed identity without duplicate slot selector',()=>{
  const nodes=new Map();let connected,received,sent,socket;
  class Terminal {open(){}onData(fn){received=fn;}onResize(){}write(){}dispose(){}loadAddon(){}get cols(){return 80;}get rows(){return 24;}}
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    Terminal,FitAddon:class {fit(){}},
    location:{hash:'#/login',protocol:'https:',host:'console.example.com'},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
    WebSocket:class {constructor(url){connected=url;socket=this;this.readyState=1;}send(value){sent=JSON.parse(value);}},
  });
  vm.runInContext(source(),context);
  context.parent=new Element('section');
  vm.runInContext("openTerminal(parent,'/tenants/t/bindings/a/versions/1/b/terminal/current?slot=current&expected_generation=7&environment_id=env-a')",context);
  const url=new URL(connected);
  assert.equal(url.searchParams.has('slot'),false);
  assert.equal(url.searchParams.get('expected_generation'),'7');
  assert.equal(url.searchParams.get('environment_id'),'env-a');
  assert.equal(typeof received,'function','terminal must forward control bytes');
  socket.onmessage({data:JSON.stringify({type:'ready'})});
  received('\u0003');
  assert.equal(sent.data,'\u0003');
});
test('accessible file entries expose preview and download actions',async()=>{
  const nodes=new Map();
  const context=vm.createContext({Node:Element,URLSearchParams,AbortController,setTimeout,
    location:{hash:'#/login'},window:{addEventListener(){}},navigator:{},
    document:{querySelector(id){if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);},createElement(tag){return new Element(tag);},createTextNode(text){return text;}},
    fetch:async url=>({ok:true,status:200,json:async()=>url.includes('/mounts?')?{mounts:[{mount_id:'fixed',target:'/home',availability:'available'}]}:{entries:[{name:'readme.txt',kind:'file',size:10,accessible:true}]}}),
  });
  vm.runInContext(source(),context);
  context.parent=new Element('section');
  await vm.runInContext("files(parent,'/tenants/t/bindings/a/versions/1/b',new URLSearchParams({slot:'current',expected_generation:'1',environment_id:'env'}))",context);
  const all=[];const visit=node=>{if(node instanceof Element){all.push(node);node.children.forEach(visit);}};visit(context.parent);
  assert.ok(all.some(node=>node.tagName==='button'&&node.textContent==='预览'),'preview button missing');
  assert.ok(all.some(node=>node.tagName==='a'&&node.textContent==='下载'&&node.href.includes('mount_id=fixed')),'download link missing fixed context');
});
test('file preview replaces previous content and ignores superseded responses',async()=>{
  const pending=[];
  const h=harness(async url=>{
    if(url.includes('/mounts?'))return response({mounts:[{mount_id:'fixed',target:'/data/state',availability:'available'}]});
    if(url.includes('mode=read'))return new Promise(resolve=>pending.push(resolve));
    return response({entries:[{name:'process.log',kind:'file',size:10,accessible:true}]});
  });
  h.context.parent=new Element('section');
  await h.run("files(parent,'/binding',new URLSearchParams({slot:'current'}))");
  const preview=()=>descendants(h.context.parent).find(n=>n.tagName==='button'&&n.textContent==='预览').onclick();
  const first=preview(),second=preview();
  pending[1](response({text:'latest output'}));await second;
  pending[0](response({text:'old output'}));await first;
  let panels=descendants(h.context.parent).filter(n=>n.tagName==='pre');
  assert.equal(panels.length,1);assert.equal(panels[0].textContent,'latest output');
  const third=preview();pending[2](response({text:'refreshed output'}));await third;
  panels=descendants(h.context.parent).filter(n=>n.tagName==='pre');
  assert.equal(panels.length,1);assert.equal(panels[0].textContent,'refreshed output');
});
