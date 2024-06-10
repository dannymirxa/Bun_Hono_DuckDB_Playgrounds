// const duckdb = require("duckdb")
import { Database } from "duckdb-async";
import { Hono } from "hono";
import { HTTPException } from 'hono/http-exception';
import { z } from "zod";

function currentDatetime() {
    const datetime: string = new Date().toISOString()
    return datetime.substring(0, 10) + ' ' + datetime.substring(11, 19)
}

const updateSchema = z.object({
    countrycode: z.string(),
    le_code: z.string().nullable(),
    distributor_code: z.string(),
    distributor_name: z.string(),
    dsr_code: z.string(),
    dsr_name: z.string(),
    lead_source: z.string(),
    dsr_assigned_date: z.string().pipe( z.coerce.date() ).nullable().default(currentDatetime()),
    business_name: z.string(),
    job_title: z.string().nullable(),
    business_type: z.string(),
    converted_to_sales: z.boolean(),
    converted_date: z.string().pipe( z.coerce.date() ).nullable().default(currentDatetime()),
    reason_for_conversion: z.string().nullable(),
    converted_vs_created_date: z.number().int().nullable(),
    mlead_assigned_vs_created_date: z.number().int().nullable(),
    is_lead_deleted: z.boolean(),
    deleted_date: z.string().nullable(),
    reason_for_deletion: z.string().nullable().optional(),
    contact_name: z.string(),
    contact_last_name: z.string().nullable(),
    contact_email: z.string().email().nullable(),
    contact_phone: z.string().nullable(),
    address: z.string(),
    city: z.string().nullable(),
    post_code: z.string(),
    // create_ts: z.string().datetime( { offset: true } ).pipe( z.coerce.date() ).default(currentDatetime()),
    create_ts: z.string().refine(date => !isNaN(Date.parse(date)), {
        message: "Invalid date format",
      }),
    // update_ts: z.string().datetime().default(currentDatetime())
    update_ts: z.string().refine(date => !isNaN(Date.parse(date)), {
        message: "Invalid date format",
      }),
})

const insertSchema= updateSchema.extend({
    id: z.number().int(),
})

const db = await Database.create("DSR.db");
const con = await db.connect();

const DSRentry = new Hono()

/* CREATE TABLE DSR (
    countrycode TEXT,
    id INTEGER PRIMARY KEY,
    le_code TEXT,
    distributor_code TEXT,
    distributor_name TEXT,
    dsr_code TEXT,
    dsr_name TEXT,
    lead_source TEXT,
    dsr_assigned_date DATE,
    business_name TEXT,
    job_title TEXT,
    business_type TEXT,
    converted_to_sales BOOLEAN,
    converted_date DATE,
    reason_for_conversion TEXT,
    converted_vs_created_date INTEGER,
    mlead_assigned_vs_created_date INTEGER,
    is_lead_deleted BOOLEAN,
    deleted_date DATE,
    reason_for_deletion TEXT,
    contact_name TEXT,
    contact_last_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    address TEXT,
    city TEXT,
    post_code TEXT,
    create_ts TIMESTAMP,
    update_ts TIMESTAMP
); */

DSRentry.get('/getAllDSR', async (c) => {
    try {
        
        const rows = await db.all("SELECT * FROM DSR");
        return c.json(rows);
    } catch (error: any) {
        throw new HTTPException(400, { message: error.message });
    }
    
})

DSRentry.get('/getDSRbyID/:id' ,async (c) => {
    const id = c.req.param('id')
    const row = await db.all(`SELECT * FROM DSR WHERE id = ${id}`);
    if (!row) {
        throw new HTTPException(400 ,{ message: "Cannot find DSR entry" });
    }
    else {
        return c.json(row)
    }
    
})

DSRentry.post('/insertNewDSR', async (c) => {
    const body = await c.req.json()

    const stmt = await con.prepare(`
        INSERT INTO DSR (
            countrycode,
            id,
            le_code,
            distributor_code,
            distributor_name,
            dsr_code,
            dsr_name,
            lead_source,
            dsr_assigned_date,
            business_name,
            job_title,
            business_type,
            converted_to_sales,
            converted_date,
            reason_for_conversion,
            converted_vs_created_date,
            mlead_assigned_vs_created_date,
            is_lead_deleted,
            deleted_date,
            reason_for_deletion,
            contact_name,
            contact_last_name,
            contact_email,
            contact_phone,
            address,
            city,
            post_code,
            create_ts,
            update_ts
        ) VALUES (?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?	, ?);`
    )

    try {
        const validationResult = insertSchema.parse(body);
        await stmt.run(
            body.countrycode,
            body.id,
            body.le_code,
            body.distributor_code,
            body.distributor_name,
            body.dsr_code,
            body.dsr_name,
            body.lead_source,
            body.dsr_assigned_date,
            body.business_name,
            body.job_title,
            body.business_type,
            body.converted_to_sales,
            body.converted_date, 
            body.reason_for_conversion,
            body.converted_vs_created_date,
            body.mlead_assigned_vs_created_date,
            body.is_lead_deleted,
            body.deleted_date,
            body.reason_for_deletion,
            body.contact_name,
            body.contact_last_name,
            body.contact_email,
            body.contact_phone,
            body.address,
            body.city,
            body.post_code,
            body.create_ts,
            body.update_ts
        );
            
        return c.json({"message": "Successfully inserted new DSR"});
    }
    catch (error: any) {
        throw new HTTPException(400, { message: error.message });
    }
})

DSRentry.put('/updateDSRbyID/:id' ,async (c) => {
    const id = c.req.param('id')
    const body = await c.req.json()
    const stmt = await con.prepare(`UPDATE DSR SET
        countrycode = ?,
        le_code = ?,
        distributor_code = ?,
        distributor_name = ?,
        dsr_code = ?,
        dsr_name = ?,
        lead_source = ?,
        dsr_assigned_date = ?,
        business_name = ?,
        job_title = ?,
        business_type = ?,
        converted_to_sales = ?,
        converted_date =?,
        reason_for_conversion = ?,
        converted_vs_created_date = ?,
        mlead_assigned_vs_created_date = ?,
        is_lead_deleted = ?,
        deleted_date = ?,
        reason_for_deletion = ?,
        contact_name = ?,
        contact_last_name = ?,
        contact_email = ?,
        contact_phone = ?,
        address = ?,
        city = ?,
        post_code = ?,
        create_ts = ?,
        update_ts = ?
        WHERE id = ${id};
        `)
    try {
        await stmt.run(
            body.countrycode,
            // body.id,
            body.le_code,
            body.distributor_code,
            body.distributor_name,
            body.dsr_code,
            body.dsr_name,
            body.lead_source,
            body.dsr_assigned_date,
            body.business_name,
            body.job_title,
            body.business_type,
            body.converted_to_sales,
            body.converted_date, 
            body.reason_for_conversion,
            body.converted_vs_created_date,
            body.mlead_assigned_vs_created_date,
            body.is_lead_deleted,
            body.deleted_date,
            body.reason_for_deletion,
            body.contact_name,
            body.contact_last_name,
            body.contact_email,
            body.contact_phone,
            body.address,
            body.city,
            body.post_code,
            body.create_ts,
            body.update_ts
        );
        return c.json({"message": `Successfully updated DSR ${id}`})
    }
    catch (error) {
        throw new HTTPException(400 ,{ message: `Problem updating DSR ${id}` });
    }
})

export default DSRentry