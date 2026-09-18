const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function dashboard(saved = new Map()) {
  const elements = new Map();
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, {
        value: id === 'gpu-search' ? '' : 'all', innerHTML: '',
        querySelectorAll() { return []; },
        handlers: {}, addEventListener(event, handler) { this.handlers[event] = handler; },
      });
      return elements.get(id);
    },
    querySelectorAll() { return []; },
  };
  const context = vm.createContext({document, console, localStorage: {
    getItem: key => saved.get(key) ?? null,
    setItem: (key, value) => saved.set(key, value),
  }});
  const source = fs.readFileSync(path.join(__dirname, 'dist/app.js'), 'utf8');
  vm.runInContext(source.replace(/\nrefresh\(\);\s*$/, '\n'), context);
  context.fixture = {
    gpus: ['lab3', 'farm9-gui2', 'cps1-model', 'cps2-model'].map(node => ({
      node, index: 0, enabled: node !== 'cps2-model', jobs: [],
    })),
    health: [{id: 'cps1-model', category: 'model', status: 'healthy'}],
  };
  vm.runInContext('current = fixture', context);
  return {context, get: id => document.getElementById(id)};
}

test('RTL validation has an independent column before build and board test', () => {
  const {context,get}=dashboard();
  get('campaign-search').value='';get('hardware-campaign-search').value='';
  context.data={campaigns:[],hardware:[{id:'rtl-demo',phase:'validation_pending',validation:{components:'succeeded',rtl:'running'},build_status:'queued',board_status:'queued',jobs:[]}]};
  vm.runInContext('renderCampaigns(data)',context);
  const html=get('hardware-campaigns').innerHTML;
  assert.match(html, /<th scope="col">Valid<\/th><th scope="col">Build<\/th><th scope="col">Board test<\/th>/);
  assert.match(html, /RTL 검증 1\/2 완료/);
  assert.match(html, /실행 중/);
  assert.doesNotMatch(html, /validation_pending|>testing</);
});

test('hardware phase is one localized badge with semantic colors', () => {
  const {context}=dashboard();
  for(const [phase,validation,label,color] of [
    ['validation_pending',{a:'queued'},'검증 대기','warn'],
    ['validation_pending',{a:'running'},'검증 중','blue'],
    ['validation_failed',{a:'failed'},'검증 실패','bad'],
    ['build_failed',{},'빌드 실패','bad'],
    ['board_running',{},'보드 테스트 중','blue'],
    ['complete',{},'완료','good'],
  ]){
    context.h={phase,validation};
    const html=vm.runInContext('hardwarePhase(h)',context);
    assert.equal(html,`<span class="badge ${color}">${label}</span>`);
  }
});

test('campaign publication distinguishes active upload from unsent backlog', () => {
  const {context}=dashboard();
  for(const [publication,label,color] of [
    [{published:14,pending:47,uploading:2,waiting:45},'업로드 중','blue'],
    [{published:14,pending:47,uploading:0,waiting:47},'업로드 대기','warn'],
    [{published:14,pending:47,uploading:0,unknown:1,waiting:46},'업로드 상태 불명','bad'],
  ]){
    context.c={recorded_state:'running',counts:{succeeded:61,cancelled:14},publication};
    const html=vm.runInContext('campaignStatusHtml(c)',context);
    assert.match(html,new RegExp(`<span class="badge ${color}">${label}<\\/span>`));
  }
  context.c={recorded_state:'running',counts:{running:1},publication:{pending:47,uploading:2}};
  assert.equal(vm.runInContext('campaignStatusHtml(c)',context),'<span class="badge blue">실행 중</span>');
});

test('RTL validation aggregates actual validation jobs, not the later build outcome', () => {
  const {context}=dashboard();
  for(const [validation,expected] of [[{},'unknown'],[{a:'queued'},'queued'],[{a:'starting'},'starting'],[{a:'succeeded',b:'running'},'running'],[{a:'succeeded',b:'failed'},'failed'],[{a:'succeeded'},'succeeded'],[{a:'cancelled'},'cancelled'],[{a:'unknown'},'unknown']]){
    context.h={validation,build_status:'failed'};
    assert.equal(vm.runInContext('validationStatus(h)',context),expected);
  }
});

test('GPU scope includes CPS', () => {
  const html = fs.readFileSync(path.join(__dirname, 'dist/index.html'), 'utf8');
  assert.match(html, /<option value="cps">CPS<\/option>/);
});

test('CPS2 unavailable badge is distinct from ordinary disabled servers', () => {
  const {context}=dashboard();
  assert.equal(vm.runInContext('badge("unavailable")',context),
               '<span class="badge ">사용 불가</span>');
  assert.equal(vm.runInContext('badge("disabled")',context),
               '<span class="badge ">배정 금지</span>');
});

test('cancelled campaigns are hidden by default with independent toggles', () => {
  const {context,get}=dashboard();
  get('campaign-search').value='';get('hardware-campaign-search').value='';
  context.data={campaigns:[
    {id:'model-cancel',recorded_state:'cancelled',jobs:[{id:'c',work_type:'train',status:'cancelled'}]},
    {id:'model-active',recorded_state:'running',jobs:[{id:'r',work_type:'train',status:'running'},{id:'c',work_type:'train',status:'cancelled'}]},
  ],hardware:[{id:'hardware-cancel',phase:'cancelled',jobs:[]},{id:'hardware-active',phase:'build_running',jobs:[]}]};
  vm.runInContext('renderCampaigns(data)',context);
  assert.doesNotMatch(get('model-campaigns').innerHTML,/model-cancel/);
  assert.match(get('model-campaigns').innerHTML,/model-active/);
  assert.doesNotMatch(get('hardware-campaigns').innerHTML,/hardware-cancel/);
  assert.match(get('hardware-campaigns').innerHTML,/hardware-active/);
  get('campaign-show-cancelled').checked=true;
  vm.runInContext('current=data',context);
  get('campaign-show-cancelled').handlers.change();
  assert.match(get('model-campaigns').innerHTML,/model-cancel/);
  assert.doesNotMatch(get('hardware-campaigns').innerHTML,/hardware-cancel/);
  get('hardware-campaign-show-cancelled').checked=true;
  get('hardware-campaign-show-cancelled').handlers.change();
  assert.match(get('hardware-campaigns').innerHTML,/hardware-cancel/);
});

test('inbox category filters all columns and counts, preserving read state', () => {
  const saved=new Map();const {context,get}=dashboard(saved);
  context.data={notifications:[
    {id:'s',campaign:'software-only',category:'model',kind:'started',created:1},
    {id:'h',campaign:'hardware-only',category:'hardware',kind:'error',created:2},
    {id:'r',campaign:'hardware-read',category:'hardware',kind:'complete',created:3,read_at:4},
  ]};
  vm.runInContext('current=data;renderInbox(data)',context);
  assert.equal(get('unread-count').textContent,'2');
  get('notification-model').checked=false;get('notification-model').handlers.change();
  assert.equal(get('unread-count').textContent,'1');
  assert.match(get('notifications').innerHTML,/hardware-only/);
  assert.doesNotMatch(get('notifications').innerHTML,/software-only|hardware-read/);
  get('unread-only').checked=false;get('unread-only').handlers.change();
  assert.match(get('notifications').innerHTML,/hardware-read/);
  assert.equal(context.data.notifications[2].read_at,4);
  assert.equal(dashboard(saved).get('notification-model').checked,false);
  assert.equal(dashboard(saved).get('notification-hardware').checked,true);
  get('notification-model').checked=true;get('notification-hardware').checked=false;get('notification-hardware').handlers.change();
  assert.match(get('notifications').innerHTML,/software-only/);
  assert.doesNotMatch(get('notifications').innerHTML,/hardware-only|hardware-read/);
  get('notification-hardware').checked=true;get('notification-hardware').handlers.change();
  assert.equal(get('unread-count').textContent,'2');
  get('notification-model').checked=false;get('notification-hardware').checked=false;get('notification-hardware').handlers.change();
  assert.equal(get('unread-count').textContent,'0');
  assert.doesNotMatch(get('notifications').innerHTML,/software-only|hardware-only/);
  assert.equal(dashboard(saved).get('notification-hardware').checked,false);
});

test('bulk read snapshots selected unread IDs and preserves hidden or incoming alerts', async () => {
  const {context,get}=dashboard();
  context.data={notifications:[{id:'s',category:'model',kind:'error',created:1},{id:'h',category:'hardware',kind:'error',created:2}]};
  vm.runInContext('current=data',context);get('notification-hardware').checked=false;
  let body;
  context.fetch=async (url,options)=>{assert.equal(url,'/api/notifications/read-many');body=JSON.parse(options.body);context.data.notifications.push({id:'new',category:'model',kind:'error',created:3});return {ok:true,json:async()=>({ids:['s'],read_at:10})};};
  await vm.runInContext('markSelectedRead()',context);
  assert.deepEqual(body.ids,['s']);
  assert.equal(context.data.notifications[0].read_at,10);
  assert.equal(context.data.notifications[1].read_at,undefined);
  assert.equal(context.data.notifications[2].read_at,undefined);
  assert.match(get('notifications-read-all').textContent,/\(1\)/);
});

test('bulk read failure preserves unread state', async () => {
  const {context,get}=dashboard();context.data={notifications:[{id:'s',category:'model',kind:'error',created:1}]};
  vm.runInContext('current=data',context);context.fetch=async()=>({ok:false});
  await assert.rejects(vm.runInContext('markSelectedRead()',context));
  assert.equal(context.data.notifications[0].read_at,undefined);
  assert.equal(get('notifications-read-all').disabled,false);
});

test('inbox defaults to unread only and can show read items', () => {
  const {context,get}=dashboard();
  assert.equal(get('unread-only').checked,true);
  context.data={notifications:[{id:'1',campaign:'unread-item',kind:'started',category:'model',created:1},
    {id:'2',campaign:'read-item-only',kind:'started',category:'model',created:1,read_at:2}]};
  vm.runInContext('renderInbox(data)',context);
  assert.match(get('notifications').innerHTML,/unread-item/);
  assert.doesNotMatch(get('notifications').innerHTML,/read-item-only/);
  get('unread-only').checked=false;
  vm.runInContext('renderInbox(data)',context);
  assert.match(get('notifications').innerHTML,/read-item-only/);
});

test('campaign work toggles are split into train and eval columns', () => {
  const {context,get}=dashboard();
  get('campaign-search').value='';get('hardware-campaign-search').value='';
  context.data={hardware:[],campaigns:[{id:'sample',recorded_state:'running',counts_by_type:{train:{running:1},eval:{dependency_wait:1}},jobs:[
    {id:'TRAIN_ONLY',work_type:'train',status:'running',dependencies:[]},
    {id:'EVAL_ONLY',work_type:'eval',status:'queued',dependencies:[]},
    {id:'DONE',work_type:'train',status:'succeeded',dependencies:[]},
    {id:'PREP_ONLY',work_type:'support',status:'running',dependencies:[]},
  ]}]};
  vm.runInContext('renderCampaigns(data)',context);
  const html=get('model-campaigns').innerHTML;
  const cells=html.match(/<tbody><tr>([\s\S]*?)<\/tr>/)[1].split('</td>');
  assert.doesNotMatch(cells[0],/<details/);
  assert.match(cells[3],/data-detail="train-jobs"/);
  assert.match(cells[3],/학습 작업 1개/);
  assert.match(cells[3],/TRAIN_ONLY/);assert.doesNotMatch(cells[3],/EVAL_ONLY|PREP_ONLY|DONE/);
  assert.match(cells[4],/data-detail="eval-jobs"/);
  assert.match(cells[4],/평가 작업 1개/);
  assert.match(cells[4],/EVAL_ONLY/);assert.doesNotMatch(cells[4],/TRAIN_ONLY/);
});

test('train and eval have independent stable expansion keys', () => {
  const {context}=dashboard();
  context.makeDetail=type=>({dataset:{detail:type+'-jobs'},closest:()=>({querySelector:()=>({textContent:'sample'})})});
  assert.equal(vm.runInContext("detailKey(makeDetail('train'))",context),'sample|train-jobs');
  assert.equal(vm.runInContext("detailKey(makeDetail('eval'))",context),'sample|eval-jobs');
  assert.equal(vm.runInContext("details([], 'train')",context),'');
});

test('verified legacy posterior phase is not optimizer stagnation and remains bounded', () => {
  const {context}=dashboard();
  context.j={status:'running',work_type:'train',progress:{epoch:10,optimizer_steps_executed:17878,age_s:1400,phase:'posterior',phase_inferred:true,phase_elapsed_s:1400}};
  assert.equal(vm.runInContext('jobStatus(j)',context),'running');
  assert.match(vm.runInContext('progress(j)',context),/posterior 계산/);
  context.j.progress.phase_elapsed_s=3600;
  assert.equal(vm.runInContext('jobStatus(j)',context),'postprocess_check');
  context.j.progress.phase_inferred=false;
  assert.equal(vm.runInContext('jobStatus(j)',context),'stalled');
});

test('model metric caption adds resource waiting without changing active counts', () => {
  const {context}=dashboard();
  context.summary={active_by_type:{train:34,eval:5},model_resource_waiting:12};
  assert.equal(vm.runInContext('modelRunCaption(summary)',context),'train 34 · eval 5 · R-W 12');
  context.summary.model_resource_waiting=0;
  assert.match(vm.runInContext('modelRunCaption(summary)',context),/R-W 0$/);
});

test('old optimizer progress is surfaced without rewriting the job lifecycle', () => {
  const {context}=dashboard();
  context.stuck={status:'running',work_type:'train',progress:{epoch:1,optimizer_steps_executed:20,age_s:3600}};
  assert.equal(vm.runInContext('jobStatus(stuck)',context),'stalled');
  assert.equal(vm.runInContext("matchJob(stuck,'failed')",context),true);
  assert.match(vm.runInContext("badge(jobStatus(stuck))",context),/bad.*진행 정체/);
  assert.equal(context.stuck.status,'running');
  context.stuck.progress.phase='gpu_health';
  assert.equal(vm.runInContext('jobStatus(stuck)',context),'resource_wait');
});

test('ensemble progress distinguishes the current model and displays applied updates', () => {
  const {context}=dashboard();
  context.training={status:'running',work_type:'train',progress:{epoch:3,planned_epochs:20,member:0,members:3,optimizer_steps_executed:5018,age_s:2}};
  const html=vm.runInContext('progress(training)',context);
  assert.match(html,/모델 1\/3/);
  assert.match(html,/E3 \/ 20/);
  assert.doesNotMatch(html,/updates/);
  context.training.progress.step_in_epoch=25;context.training.progress.steps_per_epoch=100;
  assert.match(vm.runInContext('progress(training)',context),/train 25%/);
  assert.doesNotMatch(html,/기록 대기/);
  context.training.progress.members=1;
  assert.doesNotMatch(vm.runInContext('progress(training)',context),/모델 1\/1/);
});

test('evaluation shows percentage without cell counts or updates',()=>{
  const {context}=dashboard();
  context.j={work_type:'eval',progress:{completed_cells:60,planned_cells:61}};
  assert.match(vm.runInContext('progress(j)',context),/eval 98.4%/);
  context.j.progress.planned_cells=0;
  assert.match(vm.runInContext('progress(j)',context),/진행률 확인 중/);
});

test('evaluation root progress stays visibly active without a known total',()=>{
  const {context}=dashboard();
  context.j={work_type:'eval',progress:{phase:'w8a8',batches:2050}};
  assert.match(vm.runInContext('progress(j)',context),/eval 2,050 batch · w8a8/);
});

test('execution profile states are distinct from resource waiting',()=>{
  const {context}=dashboard();
  context.j={status:'queued',waiting:{category:'validation_wait'}};
  assert.equal(vm.runInContext('jobStatus(j)',context),'validation_wait');
  assert.match(vm.runInContext('badge(jobStatus(j))',context),/검증 대기/);
  context.j.waiting.category='validation_failed';
  assert.equal(vm.runInContext('jobStatus(j)',context),'validation_failed');
  assert.match(vm.runInContext('badge(jobStatus(j))',context),/검증 실패/);
});

test('checkbox hides whole disabled servers, not individually forbidden GPUs', () => {
  const {context, get} = dashboard();
  context.fixture.health.push({id:'farm9-gui2',category:'model',enabled:false,status:'disabled'});
  context.fixture.health.push({id:'cps2-model',category:'model',enabled:true,status:'healthy'});
  assert.equal(get('gpu-show-disabled').checked,true);
  vm.runInContext('renderGPU(fixture)',context);
  assert.match(get('gpu-content').innerHTML,/FARM9-GUI2/);
  get('gpu-server').value='farm9-gui2';
  get('gpu-show-disabled').checked=false;
  get('gpu-show-disabled').handlers.change();
  assert.equal(get('gpu-server').value,'all');
  assert.doesNotMatch(get('gpu-server').innerHTML,/farm9-gui2/);
  assert.doesNotMatch(get('gpu-content').innerHTML,/FARM9-GUI2/);
  assert.match(get('gpu-content').innerHTML,/CPS2-MODEL/);
  assert.match(get('gpu-content').innerHTML,/사용 불가/);
  vm.runInContext('updateGPUServers(fixture); renderGPU(fixture)',context);
  assert.doesNotMatch(get('gpu-content').innerHTML,/FARM9-GUI2/);
  get('gpu-show-disabled').checked=true;
  get('gpu-show-disabled').handlers.change();
  assert.match(get('gpu-content').innerHTML,/FARM9-GUI2/);
  assert.equal(context.fixture.gpus.length,4);
});

test('disabled-server visibility preference persists on reload', () => {
  const saved=new Map();
  const first=dashboard(saved);
  first.get('gpu-show-disabled').checked=false;
  first.get('gpu-show-disabled').handlers.change();
  assert.equal(saved.get('gpu-show-disabled'),'false');
  assert.equal(dashboard(saved).get('gpu-show-disabled').checked,false);
});

test('switching LAB server selection to CPS resets it and renders CPS GPUs', () => {
  const {context, get} = dashboard();
  get('gpu-server').value = 'lab3';
  get('gpu-group').value = 'cps';
  get('gpu-group').handlers.input();
  assert.equal(get('gpu-server').value, 'all');
  assert.match(get('gpu-server').innerHTML, /cps1-model/);
  assert.match(get('gpu-server').innerHTML, /cps2-model/);
  assert.doesNotMatch(get('gpu-server').innerHTML, /lab3|farm9/);
  assert.match(get('gpu-content').innerHTML, /CPS1-MODEL/);
  assert.match(get('gpu-content').innerHTML, /CPS2-MODEL/);
  assert.match(get('gpu-content').innerHTML, /사용 불가/);
  assert.doesNotMatch(get('gpu-content').innerHTML, /LAB3|FARM9/);
  get('gpu-server').value = 'cps1-model';
  vm.runInContext('updateGPUServers(fixture)', context);
  assert.equal(get('gpu-server').value, 'cps1-model');
  get('gpu-group').value = 'all';
  get('gpu-group').handlers.input();
  assert.match(get('gpu-server').innerHTML, /lab3/);
  assert.match(get('gpu-server').innerHTML, /farm9-gui2/);
});

test('disabled GPU shows unavailable instead of an experiment, retaining telemetry', () => {
  const {context, get} = dashboard();
  const gpu = context.fixture.gpus.find(g => g.node === 'cps2-model');
  gpu.jobs = [{id: 'hidden-experiment', status: 'running'}];
  gpu.temperature_c = 42;
  gpu.used_mib = 8192;
  gpu.total_mib = 98304;
  gpu.utilization_average = {state: 'ready', percent: 75, sample_count: 5, sample_span_s: 160, partial: false};
  get('gpu-group').value = 'cps';
  get('gpu-server').value = 'cps2-model';
  vm.runInContext('renderGPU(fixture)', context);
  const html = get('gpu-content').innerHTML;
  assert.match(html, /사용 불가/);
  assert.match(html, /75%/);
  assert.match(html, /3분 평균/);
  assert.match(html, /42°C/);
  assert.match(html, /8 \/ 96G/);
  assert.doesNotMatch(html, /hidden-experiment|스케줄러 미배정/);
  assert.equal(gpu.jobs[0].id, 'hidden-experiment');
});
