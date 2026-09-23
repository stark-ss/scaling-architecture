const Redis=require('ioredis');
const{Pool}=require('pg');
require('dotenv').config();


const redis=new Redis();

async function Scheduler(){
    try{
        const now=Date.now();
        const jobs=await redis.zrangebyscore('q:delayed',0,now);

        if(jobs.length>0){
            for(const mem of jobs){
                const remove=await redis.zrem('q:delayed',mem);
                if(remove){
                    const[jobId,prior]=mem.split(':');
                    await redis.lpush(`q:jobs${prior}`,jobId);
                    console.log(`[scheduler] delayed job ${jobId} matured and moved to queue`) 
                }
            }
        }

    }catch(e){
        console.error(`[scheduler error]`,e.message);
    }
    setTimeout(Scheduler,800);
}
console.log('scheduler is running... ')
Scheduler();