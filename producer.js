const Redis=require('ioredis');
const crypto=require('crypto');
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

async function job(taskType,payload,prior,idpkey) {

    try{
        const query=`insert into jobs(task_type,payload,status,priority,idempotency_key)
        values ($1,$2,'queued',$3,$4)
        on conflict(idempotency_key) do nothing
        returning id`;
        const val=[taskType,JSON.stringify(payload),prior,idpkey];
        const res=await pool.query(query,val);

        if(res.rows.length===0){
            console.log(`[API Producer] dupliacte key detected key:${idpkey}`);
            return;
        }
        const jobID=res.rows[0].id;
        
        console.log(`[PostgreSQL] Saved job ${jobID.substring(0,5)} with priority ${prior} and status 'queued'`);
        
        await redis.lpush(`q:jobs${prior}`,jobID);
        console.log(`[API Producer] Pushed job ${jobID.substring(0,5)} to Redis queue!\n`);
    }catch(e){
        console.error('[Error creating job]:',e);
    }
}

async function run(){
    await job('send mail', { recipient: 'user@aa.com', template: 'welcome'},0,'aa0-qaq00qqssqq1aaa11241s');
    await job('send mail', { recipient: 'user@ana.com', template: 'welcome'},1,'6611aqssqqqqaqa561');
    await job('send text', { recipient: 'user@ana.com', template: 'welcome'},2,'15waaqq1assqqqq0');
    await job('send text', { recipient: 'user@ana.com', template: 'welcome'},2,'15104aqqssqqqqaaq5');
    await job('send text', { recipient: 'user@ana.com', template: 'welcome'},2,'15104aaaqqq5as4aaqa7');

    await pool.end();
     await redis.quit();
}


async function generate() {

    for(let i=1;i<=10;i++){
        let jobID=crypto.randomUUID();
        let type='mail';
        let prior=0;

        if(i%3===1)
            prior=1;
        else if(i%3===2)
            prior=2;

        if(i%5===1)
            type='text';
        else if(i%5===2)
            type='video';
        else if(i%5===3)
            type='audio';
        else if(i%5===4)
            type='chat';

        await job(type,{recipient:'addw@sf',template:'adtgg'},prior,jobID);
    }
    
}
generate();