const {fork}=require('child_process');
const Redis=require('ioredis');
const redis=new Redis();

const WORKER=`./worker.js`;
const MIN_WORKER=1;
const MAX_WORKER=5;
const LIMIT=15;
const LOW_LIMIT=3;

let workers=[];
let checks=0;
const CHECK=3;

function Scaling(a){
    if(a==='up' && workers.length<MAX_WORKER){
        const w=fork(WORKER);
        w.on('exit',()=>{workers=workers.filter(item=>item!==w);});
        workers.push(w);
        console.log(`[scaler] spawned worker.total workers ${workers.length}`);
    }
    else if(a==='down' && workers.length>MIN_WORKER){
        const w=workers.pop();
        w.send('graceful_shutdown');
        console.log(`[scaler] scaling down remaining workers ${workers.length}`);
    }

}
 async function monitor() {
    try{
        const[l2,l1,l0,prLen]=await Promise.all([
            redis.llen('q:jobs2'),
            redis.llen('q:jobs1'),
            redis.llen('q:jobs0'),
            redis.llen('q:processing')
        ]);
        const tot=l2+l1+l0;
        console.log(`[Scaler] Queued Jobs: ${tot} | Workers: ${workers.length}`);
        if(tot>LIMIT){
             Scaling('up');
             checks=0;
        }

        else if(tot<LOW_LIMIT && prLen===0){
            checks++;
            console.log(`[Scaler] Low load stability count: ${checks}/${CHECK}`);
            if(checks>=CHECK){
             Scaling('down');
             checks=0;
            }
            }
        else {
            checks=0;
        }    

        }catch(e){
            console.error(`[Scaler error],e.message`);
    }
    
    setTimeout(monitor,5000);
 }

 Scaling('up');
 monitor();

 process.on('SIGINT', () => {
    console.log(`\n[scaler] graceful shutdown`)
    workers.forEach(w => w.send('graceful_shutdown'));
    setTimeout(()=>{
    redis.quit();
    process.exit(0);},5000);
});