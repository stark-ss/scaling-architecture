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

const LUA_SCRIPT=`
local dest=KEYS[1]
local hbkey=KEYS[2]
local jobId=ARGV[1]

if redis.call('EXISTS',hbkey)==0 then
   return redis.call('LREM',dest,1,jobId)
end
return 0`;

async function watcher() {

        try{
            const processing=await redis.lrange('q:processing',0,-1);
            if(processing.length>0){
                for(const jobId of processing){
                    const hB=await redis.exists(`hb:${jobId}`);
                    if(hB) continue;

                    const dbCheck=await pool.query(`select status,attempts,max_attempts,priority,run_at from jobs where id=$1`,[jobId]);

                    if(dbCheck.rows.length===0){
                        await redis.lrem('q:processing',1,jobId);
                        continue;
                    }

                    const job=dbCheck.rows[0];

                    if(job.run_at){
                        const runTime=Date.now()-new Date(job.run_at).getTime();
                        if(runTime<3000){
                            continue;
                        }
                    }

                    const claimed=await redis.eval(
                        LUA_SCRIPT,2,
                        'q:processing',`hb:${jobId}`,
                        jobId
                    );

                    if (claimed===0) continue;

                     console.warn(`[watcher] orpahn job detected ${jobId.substring(0,5)}`);
                   
                    const attempts=job.attempts;
                    const retries=job.max_attempts;
                    const prior=job.priority??0;

                    await redis.del(`hb:${jobId}`);

                    if(attempts<retries){
                        await pool.query(`update jobs set attempts=attempts+1,status='queued',run_at=null where id=$1`,[jobId]);
                        await redis.lpush(`q:jobs${prior}`,jobId);
                        console.log(`[watcher] job ${jobId.substring(0,5)} rescued and re-queued`);
                    
                    }else{
                        await pool.query(`update jobs set attempts=attempts+1,status='failed',run_at=null where id=$1`,[jobId]);
                        await redis.lpush('q:dlq',jobId);
                        console.log(`[watcher] job ${jobId.substring(0,5)} max attempt reached pushed to DLQ`);
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