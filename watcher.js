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


const CHECK=5000;

async function watcher() {

        try{

            const activeJob=await redis.smembers('activeJob');

            if(activeJob.length>0){
                for(const jobId of activeJob){

                    const dbCheck=await pool.query(`select status,attempts,max_attempts,priority from jobs where id=$1`,[jobId]);
           
                    if(dbCheck.rows.length===0 ||dbCheck.rows[0].status!=='running'){
                        await redis.srem('activeJob',jobId);
                        continue;
                    }

                   const hB=await redis.exists(`hb:${jobId}`);
                   if(hB) continue;

                    const job=dbCheck.rows[0];
                    const currentattempt=job.attempts;
                    const retries=job.max_attempts;
                    const prior=job.priority??0;

                    console.warn(`[watcher] heartbeat lost job ${jobId.substring(0,5)} timed out `);

                    await redis.srem('activeJob',jobId);

                    if(currentattempt<retries){
                        await pool.query(`update jobs set attempts=attempts+1,status='queued',run_at=null where id=$1`,[jobId]);

                        await redis.lpush(`q:jobs${prior}`,jobId);
                        console.log(`[watcher] time out job ${jobId.substring(0,5)} re-queued`);
                    }else{
                        await pool.query(`update jobs set attempts=attempts+1,status='failed',run_at=null where id=$1`,[jobId]);

                        await redis.lpush('q:dlq',jobId);
                        console.log(`[watcher] timed-out ${jobId.substring(0,5)} pushed to dlq`)
                    }
                }

            }

        }catch(e){
            console.error('[watcher error]:',e.message);
        }
    
        setTimeout(watcher,CHECK);
    
} 
console.log('[watcher] is running...');
watcher();