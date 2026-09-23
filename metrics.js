const Redis=require('ioredis');
require('dotenv').config();
const redis=new Redis();

let prevProcess=0;
let lastTime=Date.now();

async function report() {
     setInterval(async()=>{
        try{
            const l2=await redis.llen('q:jobs2');
            const l1=await redis.llen('q:jobs1');
            const l0=await redis.llen('q:jobs0');
            const dlq=await redis.llen('q:dlq');

            const totTime=Number(await redis.get('totalTime')||0);
            const processCount=Number(await redis.get('process')||0);
            const success=Number(await redis.get('success')) ||0;
            const failure=Number(await redis.get('failure')) ||0;

            const curTime=Date.now();
            const dif=(curTime-lastTime)/1000;
            const processDif=processCount-prevProcess;
            const thrghpt=dif>0?(processDif/dif).toFixed(2):0;
             prevProcess=processCount;
             lastTime=curTime;
            

            const avgTime=processCount>0?(totTime/processCount/1000).toFixed(2):0.00;
            
            console.log(`\n---[System Metrics]---`);
            console.log(`Queue Lenghts:-jobs2:${l2} | jobs1:${l1} | jobs0:${l0} | dlq:${dlq}`);
            console.log(`Performance  total processed:${processCount} | success:${success} | failure:${failure}`);
            console.log(`Throughput  - ${thrghpt} jobs/sec`);
            console.log(`AVG Duration  ${avgTime} per job`);
            console.log(` -------------\n`);

        }catch(e){
            console.error(`failed to fetch stats`,e.message);
        }
    },10000);
}
console.log('System Metrics');
report();