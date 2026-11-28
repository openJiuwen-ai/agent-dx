const app=document.querySelector('#app'),nav=document.querySelector('#nav'),actor=document.querySelector('#actor');
let session,epoch=0,controller,terminal;
let expiryTimer;
let loginNotice='';
const listRequests=new WeakMap();
function expireSession(message='登录已过期，请重新登录。'){clearTimeout(expiryTimer);epoch++;controller?.abort();controller=new AbortController();terminal?.close();terminal=null;session=null;loginNotice=message;nav.replaceChildren();actor.textContent='';location.hash='/login';login();}
function setSession(value){session=value;clearTimeout(expiryTimer);expiryTimer=setTimeout(expireSession,Math.max(0,value.expires_at*1000-Date.now()));return value;}
const enc=encodeURIComponent,base='/api/console/v1';
function node(tag,text,attrs={}){const el=document.createElement(tag);if(text!==undefined)el.textContent=text;Object.assign(el,attrs);return el;}
function link(text,path){return node('a',text,{href:'#'+path});}
function button(text,fn){return node('button',text,{onclick:fn});}
function confirmBindingDelete(id){return prompt(`确认删除此 Binding，请输入完整 ID：${id}`)===id;}
function bindingReturn(t,name,kind,ref){const expected=`/tenants/${enc(t)}/templates/${enc(name)}`,candidate=new URLSearchParams(location.hash.split('?')[1]||'').get('return');return candidate?.split('?')[0]===expected?candidate:`${expected}?kind=${enc(kind)}&ref=${enc(ref)}`;}
function section(title,parent=app){const el=node('section');el.append(node('h2',title));parent.append(el);return el;}
async function api(path,options={}){if(path.includes('/aliases?')){const [target,raw]=path.split('?'),query=new URLSearchParams(raw);if(query.has('alias')){query.set('name',query.get('alias'));query.delete('alias');path=target+'?'+query;}}const requestEpoch=epoch;const response=await fetch(base+path,{credentials:'same-origin',signal:controller?.signal,...options,headers:{'content-type':'application/json',...options.headers}});const data=response.status===204?null:await response.json();if(requestEpoch!==epoch){const error=new Error('页面已切换');error.name='AbortError';throw error;}if(!response.ok){const error=new Error(data?.error?.message||'请求失败');error.kind=data?.error?.kind;if(response.status===401&&!(path==='/session'&&options.method==='POST')){expireSession();error.name='AbortError';}throw error;}if(path==='/session'&&data?.expires_at)setSession(data);return data;}
function failure(parent,error){if(error.name!=='AbortError')parent.append(node('p',`${error.message} (${error.kind||'error'})`,{className:'error'}));}
function fields(value,parent){const grid=node('div',undefined,{className:'kv'});for(const [key,val]of Object.entries(value||{})){grid.append(node('strong',key));if(val&&typeof val==='object'){const sub=node('div');fields(val,sub);grid.append(sub);}else grid.append(node('span',val===null?'未知':String(val)));}parent.append(grid);}
function configurationFields(value,parent){
  const rows=[];
  const visit=(value,path)=>{
    if(value!==null&&typeof value==='object'&&Object.keys(value).length){for(const [key,child]of Object.entries(value))visit(child,Array.isArray(value)?`${path}[${key}]`:path?`${path}.${key}`:key);return;}
    rows.push([path,value===null?'未知':typeof value==='object'?JSON.stringify(value):String(value)]);
  };
  visit(value,'');table(parent,['Key','Value'],rows);
}
let editorInputSequence=0;
function editValues(value,parent,field=''){
  if(value&&typeof value==='object'){
    const grid=node('div',undefined,{className:'editor-group'}),readers=new Map(),array=Array.isArray(value),dictionary=['env','labels','annotations'].includes(field);let sequence=0;
    const add=(key,val)=>{
      if(readers.has(key))throw new Error('键已存在');
      const nested=val!==null&&typeof val==='object',cell=node('div',undefined,{className:nested?'editor-row editor-nested':'editor-row'}),label=node('label',key),control=node('div',undefined,{className:'editor-control'});
      cell.append(label,control);const read=editValues(val,control,key);readers.set(key,read);
      const input=control.querySelector('input');if(!nested&&input){input.id='editor-'+(++editorInputSequence);label.htmlFor=input.id;}
      if(array||dictionary)cell.append(button(`删除 ${key}`,()=>{readers.delete(key);cell.remove();}));grid.append(cell);
    };
    for(const [key,val]of Object.entries(value)){add(key,val);sequence++;}
    if(array)grid.append(button('添加行',()=>{const defaults={service:{port:8080,protocol:'http'},mounts:{type:'bind',volume_id:'',target:'',options:[]},volumes:{},versions:{version:'1',weight:100}};add(String(sequence++),structuredClone(value[0]??defaults[field]??''));}));
    if(dictionary){const actions=node('div',undefined,{className:'editor-add'}),key=node('input',undefined,{placeholder:'新键'}),text=node('input',undefined,{placeholder:'新值'});actions.append(key,text,button('添加条目',()=>{if(!key.value||['__proto__','constructor','prototype'].includes(key.value))throw new Error('键无效');add(key.value,text.value);key.value='';text.value='';}));grid.append(actions);}
    parent.append(grid);return ()=>{if(array)return [...readers.values()].map(read=>read());const result={};for(const [key,read]of readers)Object.defineProperty(result,key,{value:read(),enumerable:true,writable:true});return result;};
  }
  const type=typeof value==='boolean'?'checkbox':typeof value==='number'?'number':'text',input=node('input',undefined,{type,value:value===null?'':String(value),checked:value===true});parent.append(input);
  return ()=>{if(type==='number'){const number=Number(input.value);if(input.value===''||!Number.isFinite(number))throw new Error('请输入有效数字');return number;}return type==='checkbox'?input.checked:value===null&&input.value===''?null:input.value;};
}
function publishEditor(t,name,snapshot){
  for(const previous of app.querySelectorAll('dialog'))previous.close();
  const dialog=node('dialog',undefined,{className:'version-dialog',ariaLabel:'创建新版本'}),header=node('div',undefined,{className:'version-dialog-header'}),area=node('div',undefined,{className:'version-dialog-body'}),footer=node('div',undefined,{className:'version-dialog-footer'}),status=node('div',undefined,{role:'status'});
  const dismiss=()=>dialog.close();dialog.onclose=()=>dialog.remove();
  header.append(node('h2','创建新版本'),button('关闭',dismiss));
  area.append(node('p',`模板 ${name}${snapshot.version?` · 基于版本 ${snapshot.version}`:''}`,{className:'workspace-description'}));
  const draft=structuredClone(snapshot);delete draft.version;const source=snapshot.version,read=editValues(draft,area);
  const submit=button('发布不可变版本',async()=>{if(submit.disabled)return;submit.disabled=true;status.replaceChildren();try{const result=await api(`/tenants/${enc(t)}/templates/${enc(name)}/versions`,{method:'POST',body:JSON.stringify({template:read(),source_version:source})});dialog.close();location.hash=`/tenants/${enc(t)}/templates/${enc(name)}?kind=versions&ref=${enc(result.template.version)}`;}catch(error){failure(status,error);}finally{submit.disabled=false;}});
  submit.className='version-publish';footer.append(status,button('取消',dismiss),submit);dialog.append(header,area,footer);app.append(dialog);dialog.showModal();
}
function aliasEditor(t,name,snapshot){
  for(const previous of app.querySelectorAll('dialog'))previous.close();
  const title=snapshot?'编辑别名权重':'创建别名',dialog=node('dialog',undefined,{className:'version-dialog alias-dialog',ariaLabel:title}),header=node('div',undefined,{className:'version-dialog-header'}),area=node('div',undefined,{className:'version-dialog-body'}),footer=node('div',undefined,{className:'version-dialog-footer'}),status=node('div',undefined,{role:'status'});
  const dismiss=()=>dialog.close();dialog.onclose=()=>dialog.remove();header.append(node('h2',title),button('关闭',dismiss));
  const config=snapshot?.config||{alias:'',seed:crypto.randomUUID(),versions:[{version:'1',weight:100}]},alias=node('input',undefined,{value:config.alias,placeholder:'别名',readOnly:!!snapshot}),label=node('label','别名'),identity=node('div',undefined,{className:'alias-identity'});label.append(alias);identity.append(label);
  area.append(node('p',`模板 ${name} · revision ${snapshot?.revision||'新建'}`,{className:'workspace-description'}),identity,node('p',`seed: ${config.seed}`,{className:'muted alias-seed'}));
  const rows=node('div',undefined,{className:'alias-rows'});area.append(rows);const entries=[];
  const add=entry=>{const row=node('div',undefined,{className:'alias-weight-row'}),version=node('input',undefined,{value:entry.version,placeholder:'版本'}),weight=node('input',undefined,{value:String(entry.weight),type:'number',min:0}),versionLabel=node('label','版本'),weightLabel=node('label','权重');versionLabel.append(version);weightLabel.append(weight);const record={version,weight,row};entries.push(record);row.append(versionLabel,weightLabel,button('移除',()=>{entries.splice(entries.indexOf(record),1);row.remove();}));rows.append(row);};
  config.versions.forEach(add);area.append(button('增加版本',()=>add({version:'1',weight:100})));
  const submit=button('保存权重',async()=>{if(submit.disabled)return;submit.disabled=true;status.replaceChildren();try{await api(`/tenants/${enc(t)}/templates/${enc(name)}/aliases/${enc(alias.value)}`,{method:'PUT',body:JSON.stringify({seed:config.seed,expected_revision:snapshot?.revision||null,versions:entries.map(row=>({version:row.version.value,weight:Number(row.weight.value)}))})});dialog.close();const destination=`/tenants/${enc(t)}/templates/${enc(name)}?kind=aliases&ref=${enc(alias.value)}`;if(location.hash.replace(/^#/,'')===destination)await route();else location.hash=destination;}catch(error){failure(status,error);}finally{submit.disabled=false;}});
  submit.className='version-publish';footer.append(status,button('取消',dismiss),submit);dialog.append(header,area,footer);app.append(dialog);dialog.showModal();
}
function table(parent,columns,rows){const t=node('table'),head=node('tr');columns.forEach(c=>head.append(node('th',c)));t.append(head);for(const row of rows){const tr=node('tr');for(const cell of row){const td=node('td');td.append(cell instanceof Node?cell:document.createTextNode(cell??'未知'));tr.append(td);}t.append(tr);}parent.append(t);if(!rows.length&&columns[0]==='Binding'){const empty=node('div',undefined,{className:'template-empty'});empty.append(node('span','◇'),node('h3','暂无 Binding'),node('p','当前引用下没有匹配的绑定，可调整查询条件。'));parent.append(empty);}}
async function guarded(parent,fn){try{await fn();}catch(error){failure(parent,error);}}
function filters(parent,names,load){const form=node('form'),inputs={};for(const name of names){const input=node('input',undefined,{placeholder:name,name});inputs[name]=input;form.append(input);}form.append(node('button','查询',{type:'submit'}));form.onsubmit=e=>{e.preventDefault();const q=new URLSearchParams();for(const [key,input]of Object.entries(inputs))if(input.value)q.set(key,input.value);return load(q);};parent.append(form);return new URLSearchParams();}
function bindingStateQueue(isCurrent){
  const jobs=[];let active=0;
  const drain=()=>{if(!isCurrent()){jobs.length=0;return;}while(active<4&&jobs.length){const {path,cell}=jobs.shift();active++;(async()=>{
    try{const value=await api(path);if(isCurrent())cell.textContent=({absent:'不存在',unknown:'未知'})[value.environment?.status]||value.environment?.status||'未知';}
    catch(error){if(isCurrent()&&error.name!=='AbortError')cell.textContent=['generation_conflict','environment_context_conflict'].includes(error.kind)?'绑定已变化，请刷新':'未知';}
    finally{if(isCurrent())cell.className='binding-state '+(cell.textContent==='running'?'is-running':cell.textContent==='paused'?'is-paused':['未知','不存在','绑定已变化，请刷新'].includes(cell.textContent)?'is-warning':'is-neutral');active--;drain();}
  })();}};
  return (path,cell)=>{jobs.push({path,cell});drain();};
}
function bindingListRow(value,t,name,kind,ref,returnPath,enqueue){
  const b=value.binding||value,s=b.scope,detail=link('查看详情 →',`/tenants/${enc(t)}/bindings/${enc(name)}/${kind}/${enc(ref)}/${enc(s.binding_id)}?return=${enc(returnPath)}`);detail.className='tenant-enter';
  const binding=node('div',undefined,{className:'binding-list-identity'});
  binding.append(node('code',s.binding_id,{className:'binding-primary',title:s.binding_id}),node('span',`generation · ${b.generation??'未知'}`,{className:'binding-generation',title:String(b.generation??'未知')}));
  const cells=[];
  for(const slot of ['current','secondary']){
    const env=slot==='current'?b.current:b.secondary?.environment,identity=node('div',undefined,{className:'binding-list-identity'}),state=node('span',env?'查询中':'—',{className:'binding-state is-neutral',role:'status'});
    if(env){const heading=node('div',undefined,{className:'binding-environment-heading'});heading.append(node('span',`v${env.version}`,{className:'tenant-badge'}),state);identity.append(heading,node('code',env.environment_id,{className:'binding-environment-id',title:env.environment_id}));if(slot==='secondary')identity.append(node('span',b.secondary.intent,{className:'binding-intent'}));
      const query=new URLSearchParams({expected_generation:b.generation,environment_id:env.environment_id});
      enqueue(`/tenants/${enc(t)}/bindings/${enc(name)}/${kind}/${enc(ref)}/${enc(s.binding_id)}/environment/${slot}?${query}`,state);
    }else {identity.textContent='未配置';identity.className+=' is-empty';}
    cells.push(identity);
  }
  return [binding,node('span',b.phase??'未知',{className:'tenant-badge'+(b.phase==='active'?' is-business':'')}),...cells,detail];
}
async function list(parent,path,key,columns,row,query=new URLSearchParams(),history=[]){const current=epoch,sequence=(listRequests.get(parent)?.sequence||0)+1,previous=listRequests.get(parent);listRequests.set(parent,{sequence,area:previous?.area});const value=await api(path+'?'+query);if(current!==epoch||listRequests.get(parent)?.sequence!==sequence)return;previous?.area?.remove();const area=node('div');listRequests.set(parent,{sequence,area});parent.append(area);table(area,columns,value[key].map(row));for(const anchor of area.querySelectorAll('a')){if(!anchor.href?.startsWith('#'))continue;const [target,raw]=anchor.href.slice(1).split('?'),params=new URLSearchParams(raw);if(key==='bindings'&&params.has('return')){const [back,backRaw]=params.get('return').split('?'),backQuery=new URLSearchParams(backRaw);if(query.has('page_token'))backQuery.set('binding_page_token',query.get('page_token'));else backQuery.delete('binding_page_token');params.set('return',back+'?'+backQuery);}else if(key==='versions'||key==='aliases'){if(query.has('page_token'))params.set('page_token',query.get('page_token'));else params.delete('page_token');}anchor.href='#'+target+'?'+params;}area.append(node('p',`总数 ${value.total}`));if(history.length)area.append(button('上一页',async()=>{const previous=history.at(-1);await guarded(parent,()=>list(parent,path,key,columns,row,new URLSearchParams(previous),history.slice(0,-1)));}));if(value.next_page_token)area.append(button('下一页',async()=>{const next=new URLSearchParams(query);next.set('page_token',value.next_page_token);await guarded(parent,()=>list(parent,path,key,columns,row,next,[...history,query.toString()]));}));}
function login(){
  document.title='ADX 管理台';
  app.replaceChildren();app.className='login-page';nav.hidden=true;actor.textContent='';document.querySelector('#logout').hidden=true;
  const shell=node('div',undefined,{className:'login-shell'}),brand=node('aside',undefined,{className:'login-brand'});
  const lockup=node('div',undefined,{className:'login-lockup'}),wordmark=node('div','Agent DX',{className:'login-wordmark'});wordmark.append(node('span','CONSOLE'));lockup.append(node('span','A',{className:'login-monogram'}),wordmark);
  const story=node('div',undefined,{className:'login-story'}),headline=node('h2','从模板发布\n到运行状态。',{className:'brand-headline'});headline.append(node('span','尽在掌握。'));
  story.append(node('div','YOUR AGENTS, IN VIEW',{className:'brand-eyebrow'}),headline,node('p','查询版本，管理别名，查看每个 AgentBinding\n及其关联 Environment 的当前状态。',{className:'brand-description'}));
  const flow=node('div',undefined,{className:'brand-flow'});for(const [index,text]of ['Template','AgentBinding','Environment'].entries()){if(index)flow.append(node('i','→'));flow.append(node('span',text));}story.append(flow);brand.append(lockup,story,node('div','Agent DX administration',{className:'brand-footer'}));
  const area=node('section',undefined,{className:'login-card'}),form=node('form'),kind=node('select',undefined,{id:'login-kind',value:'tenant_admin'}),id=node('input',undefined,{id:'login-id',placeholder:'请输入管理员或租户 ID',required:true,autocomplete:'username'}),token=node('input',undefined,{id:'login-token',placeholder:'请输入管理凭据',type:'password',required:true,autocomplete:'current-password'}),submit=node('button','登录管理台',{type:'submit',className:'login-submit'}),notice=node('p',loginNotice,{className:'login-notice',role:'status'});
  area.append(node('p','WORKSPACE ACCESS',{className:'login-eyebrow'}),node('h1','进入管理台'),node('p','使用您的管理身份，连接 Agent DX 工作空间。',{className:'login-subtitle'}),notice);
  for(const [value,text]of [['tenant_admin','租户管理员'],['platform_admin','平台管理员']])kind.append(node('option',text,{value}));
  for(const [text,input]of [['登录身份',kind],['账号 ID',id],['管理凭据',token]]){const label=node('label',text,{htmlFor:input.id});form.append(label,input);}form.append(submit);
  form.onsubmit=async e=>{e.preventDefault();if(submit.disabled)return;submit.disabled=true;submit.textContent='正在验证身份…';notice.textContent='';notice.className='login-notice';try{const body={login_type:kind.value,token:token.value,[kind.value==='platform_admin'?'admin_id':'tenant_id']:id.value.trim()};session=await api('/session',{method:'POST',body:JSON.stringify(body)});loginNotice='';token.value='';location.hash=session.landing_path;}catch(error){if(error.name!=='AbortError'){notice.className='login-notice is-error';notice.textContent=error.kind==='invalid_credentials'?'账号或管理凭据不正确，请检查后重试。':error.kind==='origin_forbidden'?'访问地址与服务配置不一致，请联系管理员。':'登录暂未成功，请检查网络或稍后重试。';}}finally{token.value='';submit.disabled=false;submit.textContent='登录管理台';}};
  area.append(form,node('p','管理凭据仅用于身份验证，请妥善保管。',{className:'login-footnote'}));shell.append(brand,area);app.append(shell);
}
async function tenants(){
  app.className='tenant-page';
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div');
  intro.append(node('p','WORKSPACE DIRECTORY',{className:'workspace-eyebrow'}),node('h1','租户工作空间'),node('p','选择一个租户，管理模板、版本与运行环境。',{className:'workspace-description'}));
  heading.append(intro,node('span','平台管理视图',{className:'workspace-context'}));app.append(heading);
  const area=section('租户目录');area.className='tenant-panel';
  area.append(node('p','按租户 ID、名称或类型筛选，快速定位工作空间。',{className:'workspace-description'}));
  const form=node('form',undefined,{className:'tenant-filters'}),inputs={};
  for(const [name,title,placeholder]of [['tenant_id','租户 ID','输入完整租户 ID'],['name','租户名称','搜索租户名称'],['type','租户类型','']]){
    const label=node('label',title),input=node(name==='type'?'select':'input',undefined,{name,placeholder,value:''});
    if(name==='type')for(const [value,text]of [['','全部类型'],['standard','标准租户'],['digitalmate','DigitalMate']])input.append(node('option',text,{value}));
    inputs[name]=input;label.append(input);form.append(label);
  }
  const submit=node('button','查询租户',{type:'submit',className:'tenant-search'}),reset=node('button','重置',{type:'button'}),status=node('p','',{className:'tenant-status',role:'status'});form.append(submit,reset);area.append(form,status);
  const load=query=>list(area,'/tenants','tenants',['租户 ID','租户名称','租户类型','操作'],t=>{
    const identity=node('span',t.tenant_id,{className:'tenant-id'}),name=node('div',undefined,{className:'tenant-name'}),badge=node('span',t.type==='standard'?'标准租户':t.type==='digitalmate'?'DigitalMate':t.type,{className:t.type==='digitalmate'?'tenant-badge is-business':'tenant-badge'}),enter=link('进入工作空间 →',`/tenants/${enc(t.tenant_id)}/templates`);
    name.append(node('span',(t.display_name||t.tenant_id).slice(0,1).toUpperCase(),{className:'tenant-avatar'}),node('strong',t.display_name||t.tenant_id));enter.className='tenant-enter';return [identity,name,badge,enter];
  },query);
  const search=async query=>{if(submit.disabled)return;submit.disabled=true;submit.textContent='正在查询…';status.textContent='';try{await load(query);}catch(error){if(error.name!=='AbortError')status.textContent='租户目录暂时无法加载，请重新查询。';}finally{submit.disabled=false;submit.textContent='查询租户';}};
  form.onsubmit=e=>{e.preventDefault();const query=new URLSearchParams();for(const [name,input]of Object.entries(inputs))if(input.value.trim())query.set(name,input.value.trim());return search(query);};
  reset.onclick=()=>{for(const input of Object.values(inputs))input.value='';return search(new URLSearchParams());};
  await search(new URLSearchParams());
}
async function userList(t,parent=app){
  const area=section('业务用户',parent),path=`/tenants/${enc(t)}/users`,status=node('div',undefined,{role:'status',className:'tenant-status'});
  area.className='tenant-panel';
  area.append(node('p','按用户 ID、显示名称、登录来源或账号状态筛选，查看额度与 Agent 关联。',{className:'workspace-description'}));
  const load=async query=>{if(submit.disabled)return;submit.disabled=true;submit.textContent='正在查询…';status.replaceChildren();try{await list(area,path,'users',['用户','来源','状态','模型 Key','Agent 关联','操作'],u=>{
    const identity=node('div',undefined,{className:'tenant-name'}),text=node('div'),target=`/tenants/${enc(t)}/users/${enc(u.user_id)}`;
    text.append(link(u.display_name||u.user_id,target),node('p',u.user_id,{className:'user-directory-id'}));identity.append(node('span',(u.display_name||u.user_id).slice(0,1).toUpperCase(),{className:'tenant-avatar'}),text);
    const badge=(value,ok=false)=>node('span',value,{className:ok?'tenant-badge is-business':'tenant-badge'}),enter=link('查看详情 →',target);enter.className='tenant-enter';
    return [identity,u.provider_label||u.provider||'未知',badge(({active:'正常',disabled:'已禁用',deleted:'已删除'})[u.status]||'未知',u.status==='active'),badge(({ready:'已就绪',pending:'待就绪',absent:'未配置',unknown:'未知'})[u.key_state]||'未知',u.key_state==='ready'),badge(({exists:'已关联',not_created:'未关联',uncertain:'待确认',unknown:'未知'})[u.binding_state]||'未知'),enter];
  },query);const result=listRequests.get(area)?.area;if(result&&!result.querySelectorAll('td').length){const empty=node('div',undefined,{className:'template-empty'});empty.append(node('span','◎'),node('h3','当前没有匹配的用户'),node('p','请调整筛选条件，或确认业务用户已完成登录建档。'));result.append(empty);}}catch(error){failure(status,error);}finally{submit.disabled=false;submit.textContent='查询用户';}};
  const form=node('form',undefined,{className:'tenant-filters user-filters'}),inputs=[];
  for(const [name,title,options]of [['user_id','用户 ID'],['display_name','显示名称'],['provider','登录来源',[['','全部来源'],['huawei','华为'],['agc','AGC']]],['status','账号状态',[['','全部状态'],['active','正常'],['disabled','已禁用'],['deleted','已删除']]]]){
    const input=node(options?'select':'input',undefined,{name,value:'',placeholder:title}),label=node('label',title);
    if(options)for(const [value,text]of options)input.append(node('option',text,{value}));
    inputs.push(input);label.append(input);form.append(label);
  }
  const submit=node('button','查询用户',{type:'submit',className:'tenant-search'});form.append(submit);area.append(form,status);
  form.onsubmit=event=>{event.preventDefault();const query=new URLSearchParams();for(const input of inputs)if(input.value)query.set(input.name,input.value);return load(query);};
  const reset=button('重置',()=>{for(const input of inputs)input.value='';return load(new URLSearchParams());});reset.type='button';form.append(reset);await load(new URLSearchParams());
}
function workspaceTabs(t,active,business){
  const tabs=node('div',undefined,{className:'workspace-tabs',role:'navigation',ariaLabel:'租户工作空间'});
  for(const [kind,title]of [['templates','模板工作空间'],...(business?[['users','用户管理台']]:[])]){
    const item=link(title,`/tenants/${enc(t)}/${kind}`);item.className=kind===active?'template-tab is-active':'template-tab';if(kind===active)item.ariaCurrent='page';tabs.append(item);
  }app.append(tabs);
}
async function userWorkspace(t){
  const tenant=await api(`/tenants/${enc(t)}`);app.className='tenant-page template-page user-workspace';document.title=`/${t}/用户管理台`;
  workspaceTabs(t,'users',tenant.modules?.business);
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div');intro.append(node('p','USER MANAGEMENT',{className:'workspace-eyebrow'}),node('h1','用户管理台'),node('p','查询业务用户，查看模型凭据、额度与 Agent 关联。',{className:'workspace-description'}));heading.append(intro,node('span',tenant.display_name||t,{className:'workspace-context'}));app.append(heading);await userList(t);
}
async function userDetail(t,id){
  const path=`/tenants/${enc(t)}/users/${enc(id)}`,data=await api(path),user=data.user,key=data.litellm.key,quota=data.quota;
  app.className='tenant-page template-page user-detail';document.title=`/${t}/用户${user.display_name||id}`;
  workspaceTabs(t,'users',true);
  const back=link('← 返回用户管理台',`/tenants/${enc(t)}/users`);back.className='template-back';app.append(back);
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div'),title=node('div',undefined,{className:'user-title'});
  const status={active:'正常',disabled:'已禁用',deleted:'已删除'}[user.status]||user.status||'未知';
  title.append(node('h1',user.display_name||id),node('span',status,{className:user.status==='active'?'tenant-badge is-business':'tenant-badge'}));
  intro.append(node('p','USER WORKSPACE',{className:'workspace-eyebrow'}),title,node('p','查看用户身份、模型访问凭据与 Agent 关联。',{className:'workspace-description'}));heading.append(intro,node('span',t,{className:'workspace-context'}));app.append(heading);
  if(data.degraded?.length)app.append(node('p','部分数据暂不可用，请稍后刷新重试。',{className:'user-notice',role:'status'}));
  const card=(title,description,parent=app)=>{const area=section(title,parent);area.className='tenant-panel';area.append(node('p',description,{className:'workspace-description'}));return area;};
  const details=(area,rows)=>{const list=node('dl',undefined,{className:'user-fields'});for(const [label,value]of rows)list.append(node('dt',label),node('dd',value==null||value===''?'未提供':String(value)));area.append(list);};
  const info=card('基本信息','业务身份与所属认证范围。');
  details(info,[['用户 ID',user.user_id],['显示名称',user.display_name],['登录来源',user.provider_label||user.provider],['账号状态',status],['创建时间',user.created_at],['身份范围',user.identity_scope?.developer_scope],['客户端 ID',user.identity_scope?.client_id]]);
  const budget=card('额度概览','当前模型访问额度，以积分计量。'),quotaContent=node('div'),quotaStatus=node('p','',{className:'tenant-status',role:'status'});
  const renderQuota=quota=>{quotaContent.replaceChildren();const metrics=node('div',undefined,{className:'user-metrics'});
  for(const [field,label]of [['total','总额度'],['used','已用额度'],['balance','可用余额']]){
    const state=quota.field_states?.[field],value=quota[field],metric=node('div',undefined,{className:'user-metric'});
    const display=state==='failed'?'查询失败':value==null?'未知':String(value);
    metric.append(node('span',label),node('strong',display),node('small',value==null||state==='failed'?'等待有效额度数据':'积分'));metrics.append(metric);
  }
  const dateText=value=>{if(value==null||value==='')return null;const date=new Date(typeof value==='number'?value*1000:value);if(!Number.isFinite(date.getTime()))return '未知';return date.toLocaleString('zh-CN',{year:'numeric',month:'long',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});};
  quotaContent.append(metrics);details(quotaContent,[['重置周期',quota.reset_period],['下次重置',dateText(quota.reset_at)],['查询时间',dateText(quota.queried_at)]]);};
  let quotaNoticeTimer;
  const refreshQuota=button('刷新额度',async()=>{if(refreshQuota.disabled)return;clearTimeout(quotaNoticeTimer);refreshQuota.disabled=true;refreshQuota.textContent='正在刷新…';quotaStatus.textContent='';try{
    const latest=await api(path);if(!latest.quota||latest.quota.state==='failed')throw new Error('额度查询暂不可用');renderQuota(latest.quota);quotaStatus.textContent='额度已刷新。';quotaNoticeTimer=setTimeout(()=>{quotaStatus.textContent='';},3000);
  }catch(error){if(error.name!=='AbortError')quotaStatus.textContent='刷新失败，保留上次查询结果，请重试。';}finally{refreshQuota.disabled=false;refreshQuota.textContent='刷新额度';}});
  const quotaHeading=node('div',undefined,{className:'user-quota-heading'}),quotaTitle=budget.querySelector('h2');quotaTitle.remove();quotaHeading.append(quotaTitle,refreshQuota);budget.replaceChildren(quotaHeading,...budget.children,quotaContent,quotaStatus);renderQuota(quota);
  const keyArea=card('模型 Key','凭据默认脱敏展示；查看明文后将在 15 秒内自动隐藏。');
  if(key){
    const keySummary=node('div',undefined,{className:'user-key-summary'});keySummary.append(node('code',key.key_display||'暂无脱敏信息'),node('span',key.config_status==='ready'?'已就绪':key.config_status||'未知',{className:'tenant-badge is-business'}));keyArea.append(keySummary);
    details(keyArea,[['Key 别名',key.alias],['Key ID',key.key_id],['模型用户 ID',data.litellm.user_id],['到期时间',key.expires_at]]);
    const actions=node('div',undefined,{className:'binding-actions'}),secret=node('div',undefined,{className:'user-key-secret'}),notice=node('p','',{className:'tenant-status',role:'status'});let timer;
    for(const purpose of ['view','copy']){
      const b=button(purpose==='view'?'查看明文':'复制 Key',async()=>{if(b.disabled)return;b.disabled=true;notice.textContent='';try{
        const result=await api(path+'/key/reveal',{method:'POST',body:JSON.stringify({key_id:key.key_id,purpose})});
        if(purpose==='copy'){await navigator.clipboard.writeText(result.key);notice.textContent='Key 已复制。';}
        else{clearTimeout(timer);for(const input of secret.querySelectorAll('input'))input.value='';const display=node('input',undefined,{value:result.key,readOnly:true,ariaLabel:'模型 Key 明文'});secret.replaceChildren(display);timer=setTimeout(()=>{display.value='';display.remove();},15000);}
      }catch(error){failure(notice,error);}finally{b.disabled=!key.recoverable;}});
      b.disabled=!key.recoverable;if(purpose==='view')b.className='tenant-search';actions.append(b);
    }
    keyArea.append(actions,secret,notice);
    if(!key.recoverable)keyArea.append(node('p','当前 Key 不可恢复，暂时无法查看或复制。',{className:'user-notice'}));
  }else keyArea.append(node('p',data.litellm.key_state==='absent'?'尚无 Key':'Key 查询暂不可用',{className:'user-notice'}));
  const related=card('Agent 关联','按业务身份查询实际 Binding，列出已关联的全部版本与别名。');
  if(!data.agent_associations?.length){const empty=node('div',undefined,{className:'template-empty'});empty.append(node('h3','暂无 Agent 关联'),node('p','该用户当前没有可展示的 Agent 绑定。'));related.append(empty);}
  else table(related,['种类 / 模板','关联状态','绑定引用 / Binding ID'],data.agent_associations.map(item=>{
    const exists=item.existence==='exists',absent=item.existence==='not_created',refs=absent?[]:(item.refs||[]);
    const cell=node('div',undefined,{className:'user-association'});
    cell.append(node('p',exists||refs.length?'Binding ID':'预期 Binding ID',{className:'muted'}),node('code',item.binding_id),button('复制完整 ID',()=>guarded(cell,()=>navigator.clipboard.writeText(item.binding_id))));
    for(const ref of refs){
      const kind=ref.kind==='alias'?'aliases':'versions',reference=`/tenants/${enc(t)}/templates/${enc(item.template)}?kind=${kind}&ref=${enc(ref.name)}`;
      const row=node('div',undefined,{className:'binding-actions'}),refLink=link(ref.name,reference);
      refLink.className='tenant-enter';row.append(node('span',ref.kind==='alias'?'别名：':'版本：',{className:'muted'}),refLink);cell.append(row);
    }
    if(!exists)cell.append(node('p',absent?'尚未激活创建，不会出现在模板的 Binding 列表中。':'关联暂无法确认',{className:'muted'}));
    return [`${item.kind} / ${item.template}`,node('span',exists?'已存在':absent?'尚未创建':'暂无法确认',{className:exists?'tenant-badge is-business':'tenant-badge'}),cell];
  }));
}
async function templates(t){
  const tenant=await api(`/tenants/${enc(t)}`);
  document.title=`/${t}/模板`;
  app.className='tenant-page template-page';
  workspaceTabs(t,'templates',tenant.modules?.business);
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div'),context=node('span',t,{className:'workspace-context'});
  intro.append(node('p','TEMPLATE LIBRARY',{className:'workspace-eyebrow'}),node('h1','模板工作空间'),node('p','管理不可变版本与别名，查看模板关联的运行环境。',{className:'workspace-description'}));heading.append(intro,context);app.append(heading);
  const area=section('模板目录');area.className='tenant-panel';area.append(node('p','搜索模板名称，进入工作空间查看版本、别名与绑定。',{className:'workspace-description'}));
  const form=node('form',undefined,{className:'tenant-filters'}),label=node('label','模板名称'),input=node('input',undefined,{name:'name',value:'',placeholder:'搜索模板名称，例如 orbit'}),submit=node('button','查询模板',{type:'submit',className:'tenant-search'}),reset=node('button','重置',{type:'button'}),status=node('p','',{className:'tenant-status',role:'status'});
  label.append(input);form.append(label,submit,reset);area.append(form,status);
   const load=async query=>{if(submit.disabled)return;submit.disabled=true;submit.textContent='正在查询…';status.textContent='';try{await list(area,`/tenants/${enc(t)}/templates`,'templates',['模板名称','配置摘要','操作'],v=>{const identity=node('div',undefined,{className:'tenant-name'}),summary=v.configuration_summary,version=node('span',summary?`${v.name}:${v.latest_version} · CPU ${summary.cpu_millis}m · 内存 ${summary.memory_mib} MiB · 服务 ${summary.service_count} 个`:'摘要暂不可用'),enter=link('管理模板 →',`/tenants/${enc(t)}/templates/${enc(v.name)}`);identity.append(node('span','T',{className:'tenant-avatar'}),link(v.name,`/tenants/${enc(t)}/templates/${enc(v.name)}`));enter.className='tenant-enter';return [identity,version,enter];},query);
    const result=listRequests.get(area)?.area;if(result&&!result.querySelectorAll('td').length){const empty=node('div',undefined,{className:'template-empty'});empty.append(node('span','▱'),node('h3','当前没有匹配的模板'),node('p','请调整搜索条件，或确认该租户已发布模板。'));result.append(empty);}
  }catch(error){if(error.name!=='AbortError')status.textContent='模板目录暂时无法加载，请检查上游服务后重新查询。';}finally{submit.disabled=false;submit.textContent='查询模板';}};
  form.onsubmit=e=>{e.preventDefault();const query=new URLSearchParams();if(input.value.trim())query.set('name',input.value.trim());return load(query);};reset.onclick=()=>{input.value='';return load(new URLSearchParams());};
  context.textContent=tenant.display_name||t;await load(new URLSearchParams());
}
async function template(t,name,query){
  app.className='tenant-page template-page template-detail';document.title=`/${t}/模板${name}`;
  const tenant=await api(`/tenants/${enc(t)}`);workspaceTabs(t,'templates',tenant.modules?.business);
  const path=`/tenants/${enc(t)}/templates/${enc(name)}`,view=path,kind=query.get('kind')==='aliases'?'aliases':'versions';let ref=query.get('ref'),latest;
  if(kind==='versions'&&!ref&&!query.has('version')&&!query.has('page_token')){
    try{latest=await api(path);}catch(error){if(error.kind!=='template_not_found')throw error;}
    if(latest?.template?.version){ref=latest.template.version;query=new URLSearchParams(query);query.set('kind',kind);query.set('ref',ref);history.replaceState(null,'','#'+view+'?'+query);}
  }
  const back=link('← 返回模板目录',`/tenants/${enc(t)}/templates`);back.className='template-back';app.append(back);
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div');
  intro.append(node('p','TEMPLATE WORKSPACE',{className:'workspace-eyebrow'}),node('h1',name),node('p','管理模板版本与别名，查看引用下的 Binding 和配置详情。',{className:'workspace-description'}));
  heading.append(intro,node('span',t,{className:'workspace-context'}));app.append(heading);
  const area=section('版本与别名');area.className='tenant-panel';
  area.append(node('p','发布不可变版本，通过别名管理版本权重与引用。',{className:'workspace-description'}));
  const tabs=node('div',undefined,{className:'template-tabs',role:'tablist'});
  for(const [value,title]of [['versions','版本'],['aliases','别名']]){const tab=link(title,view+'?kind='+value);tab.className=value===kind?'template-tab is-active':'template-tab';tab.setAttribute?.('aria-current',value===kind?'page':'false');tabs.append(tab);}area.append(tabs);
  const filter=kind==='versions'?'version':'alias',q=new URLSearchParams();for(const key of [filter,'page_size','page_token'])if(query.has(key))q.set(key,query.get(key));
  filters(area,[filter],values=>{values.set('kind',kind);location.hash=view+'?'+values;});
  const form=area.querySelector('form');form.className='tenant-filters detail-filters';const input=form.querySelector('input');input.placeholder=kind==='versions'?'输入版本号':'输入别名';input.value=query.get(filter)||'';
  const create=button(kind==='versions'?'创建版本':'创建别名',()=>kind==='aliases'?aliasEditor(t,name):publishEditor(t,name,{name,image:'',isolation_runtime:'runc',env:{},resources:{cpu_millis:1000,memory_mib:512},mounts:[],volumes:[],service:[]}));create.type='button';create.className='tenant-search';form.append(create);
   await list(area,`${path}/${kind}`,kind,[kind==='versions'?'版本':'别名','操作'],v=>{const reference=kind==='versions'?v.version:v.config.alias;if(kind==='aliases'&&!ref){ref=reference;query=new URLSearchParams(query);query.set('kind',kind);query.set('ref',ref);history.replaceState(null,'','#'+view+'?'+query);}const next=new URLSearchParams(query);next.set('kind',kind);next.set('ref',reference);const badge=node('span',reference,{className:'tenant-badge is-business'}),select=link(ref===reference?'当前选择':'查看详情 →',view+'?'+next);select.className='tenant-enter';return [badge,select];},q);
  if(!ref)return;
  const config=latest||await api(`${path}/${kind}/${enc(ref)}`),summary=node('div',undefined,{className:'template-summary'});
  summary.append(node('p',kind==='versions'?'SELECTED VERSION':'SELECTED ALIAS',{className:'workspace-eyebrow'}),node('strong',ref),button(kind==='versions'?'根据本版本创建':'编辑别名权重',()=>kind==='versions'?publishEditor(t,name,config.template):aliasEditor(t,name,config.alias)));area.append(summary);
  const bindings=section('引用下 Binding');bindings.className='tenant-panel binding-list-panel';bindings.append(node('p','查看当前引用关联的绑定及运行环境。',{className:'workspace-description'}));
  const returnPath=view+'?'+new URLSearchParams({...Object.fromEntries(query),kind,ref}),bindingQuery=new URLSearchParams({template:name,ref_kind:kind,ref});
  for(const key of ['binding_id','page_size'])if(query.has(key))bindingQuery.set(key,query.get(key));if(query.has('binding_page_token'))bindingQuery.set('page_token',query.get('binding_page_token'));
  filters(bindings,['binding_id'],values=>{const next=new URLSearchParams(query);next.delete('binding_page_token');if(values.has('binding_id'))next.set('binding_id',values.get('binding_id'));else next.delete('binding_id');location.hash=view+'?'+next;});
  const bindingForm=bindings.querySelector('form');bindingForm.className='tenant-filters detail-filters';bindingForm.querySelector('input').placeholder='输入完整 Binding ID';bindingForm.querySelector('input').value=query.get('binding_id')||'';
  const stateEpoch=epoch;let stateSequence,enqueue;
  await list(bindings,`/tenants/${enc(t)}/bindings`,'bindings',['Binding','阶段','当前环境 current','次要环境 secondary','操作'],v=>{
    const sequence=listRequests.get(bindings)?.sequence;
    if(sequence!==stateSequence){stateSequence=sequence;enqueue=bindingStateQueue(()=>epoch===stateEpoch&&listRequests.get(bindings)?.sequence===sequence);}
    return bindingListRow(v,t,name,kind,ref,returnPath,enqueue);
  },bindingQuery);
  const configuration=section('Key / Value 配置');configuration.className='tenant-panel template-config';configurationFields(config,configuration);
  if(kind==='aliases')for(const target of config.alias.config.versions){const targetArea=section(`目标版本 ${target.version}`);targetArea.className='tenant-panel template-config';await guarded(targetArea,async()=>configurationFields(await api(`${path}/versions/${enc(target.version)}`),targetArea));}
  if(kind==='versions'){
    try{
      const catalog=await api(`${path}/aliases?page_size=1`),first=catalog.aliases?.[0]?.config?.alias;
      if(first){const tab=[...tabs.querySelectorAll('a')].find(anchor=>anchor.textContent==='别名');if(tab)tab.href='#'+view+'?'+new URLSearchParams({kind:'aliases',ref:first});}
    }catch(error){if(error.name==='AbortError')throw error;}
  }
}
async function binding(t,name,kind,ref,id){
  app.className='tenant-page template-page template-detail binding-detail';document.title=`/${t}/模板${name}/Binding ${id}`;
  const back=link('返回模板详情',bindingReturn(t,name,kind,ref));back.className='template-back';app.append(back);
  const heading=node('div',undefined,{className:'tenant-heading'}),intro=node('div');
  intro.append(node('p','AGENT BINDING',{className:'workspace-eyebrow'}),node('h1',id),node('p',`模板 ${name} · ${kind==='aliases'?'别名':'版本'} ${ref}`,{className:'workspace-description'}));heading.append(intro,node('span',t,{className:'workspace-context'}));app.append(heading);
  const path=`/tenants/${enc(t)}/bindings/${enc(name)}/${kind}/${enc(ref)}/${enc(id)}`,data=await api(path),b=data.binding;
  const identity=section('Binding 摘要');identity.className='tenant-panel template-config';configurationFields({scope:b.scope,phase:b.phase,generation:b.generation},identity);
  const environments=section('环境');environments.className='tenant-panel';const grid=node('div',undefined,{className:'binding-env-grid'});environments.append(grid);
  const storage=section('文件（只读）');storage.className='tenant-panel';storage.append(node('p','按环境选择授权目录，浏览、预览和下载文件。所有路径均相对授权根目录。',{className:'workspace-description'}));
  for(const slot of ['current','secondary']){
    const env=slot==='current'?b.current:b.secondary?.environment,area=node('div',undefined,{className:'binding-env-card'});grid.append(area);
    const title=node('div',undefined,{className:'binding-env-title'}),badge=node('span',env?'查询中':'—',{className:'tenant-badge'});title.append(node('h3',slot),badge);area.append(title);
    if(!env){area.append(node('p',slot==='secondary'?'未配置次要环境':'未配置当前环境',{className:'workspace-description'}));continue;}
    const context=new URLSearchParams({slot,expected_generation:b.generation,environment_id:env.environment_id});
    const details=node('div',undefined,{className:'template-config'});configurationFields({实际版本:env.version,environment_id:env.environment_id,'页面 generation':b.generation,...(slot==='secondary'?{intent:b.secondary.intent}:{})},details);area.append(details);
    const controls=node('div',undefined,{className:'binding-actions'}),notice=node('p',undefined,{className:'binding-env-notice'}),errors=node('div',undefined,{role:'status'}),terminalArea=node('div');
    const connect=button('连接终端',()=>openTerminal(terminalArea,`${path}/terminal/${slot}?${context}`));connect.className='tenant-search';connect.disabled=true;
    const wake=button('恢复环境',async()=>{wake.disabled=true;errors.replaceChildren();try{await api(`${path}/environment/${slot}/wake`,{method:'POST',body:JSON.stringify({expected_generation:b.generation,environment_id:env.environment_id})});await load();}catch(error){failure(errors,error);wake.disabled=false;}});wake.className='tenant-search';wake.hidden=true;
    const retry=button('重新查询',()=>load());controls.append(wake,connect,retry);area.append(controls,notice,errors,terminalArea);
    const load=async()=>{retry.disabled=true;connect.disabled=true;wake.hidden=true;errors.replaceChildren();try{
      const state=await api(`${path}/environment/${slot}`),status=state.environment.status;badge.textContent=status;badge.className=status==='running'?'tenant-badge is-business':'tenant-badge';
      connect.disabled=status!=='running';wake.hidden=status!=='paused';wake.disabled=false;
      notice.textContent=status==='running'?'已运行，可连接 Execd PTY；无需恢复。':status==='paused'?'先显式恢复此环境，再连接终端。':status==='absent'?'环境不存在，不提供恢复或重建入口。':'状态未知或环境尚未就绪，请重新查询。';
    }catch(error){if(error.name==='AbortError')return;badge.textContent=error.kind==='environment_absent'?'absent':'unknown';notice.textContent=badge.textContent==='absent'?'环境不存在，不提供恢复或重建入口。':'环境状态查询失败，不代表环境不存在，请重新查询。';failure(errors,error);}finally{retry.disabled=false;}};
    await load();
    const fileArea=node('div',undefined,{className:'binding-storage'});fileArea.append(node('h3',`${slot} · ${env.environment_id}`));storage.append(fileArea);await guarded(fileArea,()=>files(fileArea,path,context));
  }
  const main=section('危险操作');main.className='tenant-panel binding-danger-panel';main.append(node('p','需输入完整 Binding ID，提交当前 expected_generation。共享工作区数据不随 Binding 删除。',{className:'binding-env-notice'}));
  const remove=button('强制删除',()=>{if(confirmBindingDelete(id))return guarded(main,async()=>{await api(path,{method:'DELETE',body:JSON.stringify({expected_generation:b.generation})});location.hash=bindingReturn(t,name,kind,ref);});});remove.className='binding-danger';main.append(remove);
}
function openTerminal(parent,path){
  const [target,raw]=path.split('?'),query=new URLSearchParams(raw);query.delete('slot');path=target+'?'+query;
  if(terminal?.target===path&&terminal.parent===parent&&terminal.active()){terminal.focus();return;}
  terminal?.close();
   const panel=node('div',undefined,{className:'terminal-panel'}),status=node('p','正在连接终端…',{className:'terminal-status',role:'status'}),output=node('div',undefined,{className:'terminal-output'}),controls=node('div',undefined,{className:'binding-actions'}),display=new Terminal({scrollback:2000}),fit=new FitAddon();
   let ready=false,disposed=false,fullscreen=false,ws,observer;
  const send=frame=>{if(ready&&ws?.readyState===1)ws.send(JSON.stringify(frame));};
  const resize=()=>{if(disposed)return;fit.fit();send({type:'resize',cols:display.cols,rows:display.rows});};
   const release=()=>{if(disposed)return;fullscreen=false;panel.className='terminal-panel';size.textContent='适配终端尺寸';disposed=true;ready=false;observer?.disconnect();display.dispose();};
  const handle={target:path,parent,active:()=>!disposed,focus:()=>display.focus?.(),close:()=>{release();panel.remove();if(terminal===handle)terminal=null;if(ws&&ws.readyState<2)ws.close();}};
  terminal=handle;
   const size=button('适配终端尺寸',()=>{fullscreen=!fullscreen;panel.className=fullscreen?'terminal-panel is-fullscreen':'terminal-panel';size.textContent=fullscreen?'取消适配终端尺寸':'适配终端尺寸';resize();display.focus?.();});size.disabled=true;
  controls.append(button('关闭终端',()=>handle.close()),size);panel.append(status,output,controls);parent.append(panel);
  display.loadAddon(fit);display.open(output);fit.fit();
  display.onData(data=>send({type:'input',data}));display.onResize(({cols,rows})=>send({type:'resize',cols,rows}));
  const fail=message=>{if(disposed)return;status.textContent=message;status.className='terminal-status error';size.disabled=true;release();};
  try{ws=new WebSocket(`${location.protocol==='https:'?'wss:':'ws:'}//${location.host}${base}${path}`);}catch(error){fail('终端连接失败，请检查网络与终端配置后重试。');return;}
  if(typeof ResizeObserver!=='undefined'){observer=new ResizeObserver(resize);observer.observe(output);}
  ws.onmessage=event=>{
    if(disposed)return;
    let frame;try{frame=JSON.parse(event.data);}catch(error){fail('终端响应格式无效，请重新连接。');ws.close();return;}
    if(frame.type==='ready'){ready=true;status.textContent='终端已连接';size.disabled=false;resize();display.focus?.();}
    if(frame.type==='output')display.write(frame.data);
    if(frame.type==='closed'){fail(`终端已关闭：${frame.reason||'连接结束'}。请重新连接。`);ws.close();}
  };
  ws.onerror=()=>fail('终端连接失败，请检查终端 profile、环境内 helper 和 Gateway 连通性后重试。');
  ws.onclose=event=>{if(!disposed)fail(`终端连接已断开（${event.code||'未知'}），请重新连接。`);};
}
async function files(parent,path,context){const mounts=await api(path+'/files/mounts?'+context);parent.append(node('h3','共享存储只读'));if(!mounts.mounts.length)parent.append(node('p','没有可浏览的授权目录',{className:'workspace-description'}));for(const mount of mounts.mounts){if(mount.availability!=='available'){parent.append(node('p',`${mount.target}: ${mount.availability}`));continue;}const area=node('div'),previewArea=node('div');let previewSequence=0;parent.append(area);const show=async relative=>{previewSequence++;previewArea.replaceChildren();area.replaceChildren(node('h4',mount.target));
const toolbar=node('form',undefined,{className:'binding-file-toolbar'}),root=node('input',undefined,{value:mount.target,readOnly:true}),relativePath=node('input',undefined,{value:relative,placeholder:'相对授权根的目录路径'}),rootLabel=node('label','授权根'),pathLabel=node('label','相对路径'),browse=node('button','浏览',{type:'submit'});rootLabel.append(root);pathLabel.append(relativePath);toolbar.append(rootLabel,pathLabel,browse);toolbar.onsubmit=async event=>{event.preventDefault();if(browse.disabled)return;browse.disabled=true;try{await guarded(area,()=>show(relativePath.value));}finally{browse.disabled=false;}};area.append(toolbar);
const q=new URLSearchParams(context);q.set('mount_id',mount.mount_id);q.set('path',relative);q.set('mode','list');const data=await api(path+'/files?'+q);if(relative)area.append(button('返回上层',()=>guarded(area,()=>show(relative.split('/').slice(0,-1).join('/')))));table(area,['名称','类型','大小','操作'],data.entries.map(entry=>{const actions=node('span'),child=[relative,entry.name].filter(Boolean).join('/');if(entry.accessible){if(entry.kind==='dir')actions.append(button('打开',()=>guarded(area,()=>show(child))));else{const file=new URLSearchParams(q);file.set('path',child);file.set('mode','read');actions.append(button('预览',async()=>{const sequence=++previewSequence;previewArea.replaceChildren();try{const preview=await api(path+'/files?'+file);if(sequence===previewSequence)previewArea.replaceChildren(node('pre',preview.text));}catch(error){if(sequence===previewSequence)failure(previewArea,error);}}));actions.append(node('a','下载',{href:base+path+'/files/download?'+file}));}}return [entry.name,entry.kind,entry.size,actions];}));area.append(previewArea);};await show('');}}
async function route(){
  epoch++;controller?.abort();controller=new AbortController();terminal?.close();terminal=null;app.replaceChildren();nav.replaceChildren();nav.hidden=true;
  let parts=[],search='',valid=false;const raw=location.hash.slice(1);
  try{
    const split=raw.indexOf('?'),path=split<0?raw:raw.slice(0,split);search=split<0?'':raw.slice(split+1);
    parts=path.slice(1).split('/').map(decodeURIComponent);
    valid=path.startsWith('/')&&parts.every(part=>part&&!/[\/\x00-\x1f]/.test(part))&&(
      (parts[0]==='login'&&parts.length===1)||(parts[0]==='tenants'&&(
        parts.length===1||(['templates','users'].includes(parts[2])&&[3,4].includes(parts.length))||
        (parts[2]==='bindings'&&parts.length===7&&['versions','aliases'].includes(parts[4])))));
  }catch{valid=false;}
  const redirect=path=>{const target='#'+path.replace(/^#+/,'');if(location.replace)location.replace(target);else location.hash=target;};
  try{
    if(valid&&parts[0]==='login'){login();return;}
    app.className='';document.querySelector('#logout').hidden=false;
    session=await api('/session');
    if(!valid){redirect(session.landing_path);return;}
    nav.hidden=session.role!=='platform_admin';actor.textContent=`${session.subject_id} · ${session.role==='platform_admin'?'平台管理员':'租户管理员'}`;
    if(session.role==='platform_admin')nav.append(link('所有租户','/tenants'));
    if(!parts[1]){await tenants();return;}
    const t=parts[1];if(session.role==='platform_admin')nav.append(document.createTextNode(' / '),link(t,`/tenants/${enc(t)}/templates`));
    if(parts[2]==='templates'){if(parts[3])await template(t,parts[3],new URLSearchParams(search));else await templates(t);}
    else if(parts[2]==='users'){if(parts[3])await userDetail(t,parts[3]);else await userWorkspace(t);}
    else await binding(t,...parts.slice(3));
  }catch(error){if(!valid&&error.name==='AbortError'&&!session){redirect('/login');return;}failure(app,error);}
}
document.querySelector('#logout').onclick=async()=>{const control=document.querySelector('#logout');if(control.disabled)return;control.disabled=true;try{await api('/session',{method:'DELETE'});expireSession('已安全退出，请重新登录。');}catch(error){if(error.name!=='AbortError')failure(app,new Error('退出请求未完成，请重试。'));}finally{control.disabled=false;}};window.addEventListener('hashchange',route);route();
import {Terminal} from '@xterm/xterm';
import {FitAddon} from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
