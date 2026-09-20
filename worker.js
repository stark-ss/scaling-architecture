const Redis=require('ioredis');
const{Pool}=require('pg');
require('dotenv').config();


const pool=new Pool({
    user:process.env.DB_USER,
    host:process.env.DB_HOST,
    database:process.env.DB_NAME,
    password:process.env.DB_PASSWORD,
    port:process.env.DB_PORT,
});

const redis=new Redis();

let shutDown=false;

const conc=3;
const activejob=new Set();

async function concurrent(jobId,qname) {
    const timer=Date.now();
    let hbint=null;
        try{

            const maxjob=5;
            const currsec=Math.floor(Date.now()/1000);
            const key=`rate:${currsec}`;
            const count=await redis.incr(key);
            if(count===1){
                await redis.expire(key,2);
            }

            if(count>maxjob){
                console.log(`[Rate limiter] throtling job ${jobId.substring(0,5)}`);
                 await new Promise((r)=>setTimeout(r,1000));   
            }

        await pool.query(`update jobs set status='running',run_at=current_timestamp where id=$1`,[jobId]);

        await redis.set(`hb:${jobId}`,'alive','EX',20);
        
        await redis.sadd('activeJob',jobId);
      

        hbint=setInterval(async()=>{
            await redis.set(`hb:${jobId}`,'alive','EX',20);
        },10000);

        console.log(`[Worker] picked up job id:${jobId.substring(0,5)} from queue ${qname}`);

        console.log(`[PostgreSQL] Job ${jobId.substring(0,5)} status updated to 'running'`);

        await new Promise((r)=>setTimeout(r,2000));//actual api fetching
        
        if(shutDown){
            console.log(`[Worker] Shutting down instantly`);
            if(hbint) clearInterval(hbint);
            await redis.del(`hb:${jobId}`);
            return;
        }

        
        //error simulation//
        //throw new Error('Simulated worker crash during job processing!');

        await pool.query(`update jobs set status='completed',run_at=null,updated_at=now(),message='execute' where id=$1`,[jobId]);
        
        await redis.srem('activeJob',jobId);
        clearInterval(hbint);
        await redis.del(`hb:${jobId}`);
 
        await redis.incrby('totalTime',Date.now()-timer);
        await redis.incrby('process',1);
        await redis.incrby('success',1);

        console.log(`[PostgreSQL] Job ${jobId.substring(0,5)} status updated to 'completed'\n`);

        }catch(e){
        if(hbint) clearInterval(hbint);

        await redis.set(`hb:${jobId}`,'alive','EX',20);
        hbint=setInterval(async()=>{
         await redis.set(`hb:${jobId}`,'alive','EX',20);
        },10000);

        await redis.incrby('totalTime',Date.now()-timer);
        await redis.incrby('process',1);
        await redis.incrby('failure',1);

            console.error('[worker] Error',e.message);

            if(jobId){
                try{
                   const res= await pool.query(`select attempts,max_attempts,priority from jobs where id=$1`,[jobId]);

                   if(res.rows.length>0){
                    const currentattempt=res.rows[0].attempts;
                    const retries=res.rows[0].max_attempts;
                    const priority=res.rows[0].priority??0;


                    if(currentattempt<retries){
                        const delay=Math.pow(2,currentattempt+1)*1000;

                        await pool.query(`update jobs set attempts=attempts+1,status='queued' where id=$1`,[jobId]);

                        console.log(`[Retry Handler] Job ${jobId.substring(0,5)} failed (Attempt ${currentattempt+ 1}/${retries}).`);

                       setTimeout(async()=>{
                        try{
                            await redis.lpush(`q:jobs${priority}`,jobId);
                        }catch(e){
                            console.error('reque error',e);
                        }
                       },delay);
                      
                    }
                    else{
                       await pool.query(`update jobs set attempts=attempts+1,status='failed' where id=$1`,[jobId]); 

                       await redis.lpush('q:dlq',jobId);

                       console.log(`[DLQ] Job ${jobId.substring(0,5)} permanently failed after ${retries} attempts and moved to DEAD-LETTER-QUEUE.\n`);
                    }
                }
                 
                }catch(er){
                    console.error('postgres error',er);
                }finally{
                 if(hbint) clearInterval(hbint);
                 await redis.srem('activeJob',jobId);
                 await redis.del(`hb:${jobId}`);
                }
            }
        }
    }
async function worker(){

    //await redis.flushall();

    await redis.del('totalTime','success','failure','process');

    console.log(`[worker] started`);
    while(!shutDown){
    if(activejob.size>=conc){
        await Promise.race(activejob);
        continue;
    }
    const res=await redis.brpop('q:jobs2','q:jobs1','q:jobs0',5);
    if(!res || shutDown) continue;

    const jobId=res[1];

    const job=concurrent(jobId,res[0]).finally(()=>{
        activejob.delete(job);
    });
    activejob.add(job);
    }

    if(activejob.size>0){
        console.log(`[worker] shutting down`);
        await Promise.all(activejob);
    }
    await pool.end();
    await redis.quit();
    console.log(`[worker] shutdown complete`)

}



process.on('SIGINT',()=>{
    console.log('\n[SIGNAL] Received SIGINT(ctrl+c)');
    shutDown=true;
   
});
process.on('SIGTERM',()=>{
    console.log('\n[SIGNAL] Received SIGINT');
    shutDown=true;
   
});
worker();