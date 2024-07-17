'use strict';

const mysql = require("mysql");
const { resolve } = require("path");
const { rejects } = require("assert");
const { Promise } = require("xlsx-populate/lib/externals");
const iconv = require('iconv-lite');

// Initialize MySQL pool
const pool = mysql.createPool({
  host: "speakertunity.cb4c24ewq6sg.us-west-2.rds.amazonaws.com",
  user: "speakertunity",
  password: "Pr7cd(4u$6(125#!",
  database: "ETXSales",
});

// Define the Lambda handler
module.exports.paymentresponse = async (event) => {
  try {
    const Result = await FeatchPaymnetData();
    console.log("Paid Result", Result);
    const DataHRB = await FeatchPaymnetDataHRB();
    console.log("HRB Result", DataHRB);
    const Summary = await FeatchPaymnetDataSummary();
    console.log("Summary Result" , Summary);
  } catch (error) {
    console.log(error);
  }
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'Your function executed successfully!',
        input: event,
      },
      null,
      2
    ),
  };
};

// Function to generate Excel
async function GenrateExcel(ViewDataArray) {
  const timestamp = Date.now();
  const date = new Date(timestamp);

  return new Promise(async (resolve, reject) => {
    const { filename, results, columnNames } = ViewDataArray;

    if (results.length === 0) {
      const logmessege = `No Data Available ${filename} - ${date.toString()}\n`;
      resolve(logmessege);
      return;
    }

    try {
      // Create a new workbook
      const xlsxPopulate = (await import('xlsx-populate')).default;
      const workbook = await xlsxPopulate.fromBlankAsync();
      const newSheet = workbook.addSheet("paymentsheet");

      workbook.deleteSheet(workbook.sheet(0).name());

      // Set the column names
      columnNames.forEach((header, index) => {
        //const capitalizedHeader = header.charAt(0).toUpperCase() + header.slice(1);
        newSheet.cell(1, index + 1).value(header);
      });

      // Add the data
      results.forEach((row, rowIndex) => {
        Object.entries(row).forEach(([key, value], columnIndex) => {
          newSheet.cell(rowIndex + 2, columnIndex + 1).value(value === null ? "" : value);
        });
      });

      const excelbuffer = await workbook.outputAsync();

      const { Readable } = await import('stream');
      const excelStream = new Readable();
      excelStream.push(excelbuffer);
      excelStream.push(null); // Signal the end of the stream

      const uploadParams = {
        Bucket: process.env.bucket,
        Key: `${process.env.exportedexcel}/${filename}.xlsx`, // Key is the name of the file in S3
        Body: excelStream,
        ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ServerSideEncryption: "AES256",
      };

      try {
        const uploadResult = await uploadWithRetry(uploadParams);
        console.log("File uploaded successfully. File URL:", uploadResult.Location);
        resolve(`File uploaded successfully. File URL: ${uploadResult.Location}\n`);
      } catch (err) {
        console.error("Error uploading file to S3:", err);
        const logmessege = `The Error Is ${err} - ${date.toString()}\n`;
        reject(`An error occurred: ${logmessege}`);
      }

      const logmessege = `The ${filename}'s Data Saved In Excel Count Is ${results.length} On ${date.toString()}\n`;
      resolve(logmessege);

    } catch (err) {
      console.error("Error:", err);
      const logmessege = `The Error Is ${err} - ${date.toString()}\n`;
      reject(`An error occurred: ${logmessege}`);
    }
  });
}

// Function to upload with retry logic
async function uploadWithRetry(params, retries = 5) {
  const AWS = (await import('aws-sdk')).default;
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(5);

  AWS.config.update({
    secretAccessKey: process.env.secretAccessKey,
    region: process.env.region,
  });
  const s3 = new AWS.S3();
  let attempt = 0;
  let delay = 100; // Initial delay in milliseconds

  while (attempt < retries) {
    try {
      return await s3.upload(params).promise();
    } catch (err) {
      if (err.code === 'SlowDown') {
        attempt++;
        console.log(`SlowDown error, retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        delay *= 2; // Exponential backoff
      } else {
        throw err;
      }
    }
  }
  throw new Error('Max retries reached');
}

// Function to fetch payment data
async function FeatchPaymnetData() {
  return new Promise(async (resolve, rejects) => {
      const timestamp = Date.now();
    const date = new Date(timestamp);
    let sqlquery = `select sale_id as sale_id, product_id as product_id, product_name as product_name, payment_amount as payment_amount, payer_first_name as first_name, payer_last_name as last_name, payer_phone as phone, payer_email as email, payer_address_city as city, payer_address_state as state, payer_address_zip as zipcode, DATE_FORMAT(payment_time, '%m/%d/%Y %H:%i') as date, receiver as receiver, role as role, member_id as member_id, amount as amount, paid as paid, transaction_id as transaction_id,broker_id as institution_code from ETX_Prod where payment_amount != 0 and payment_time >= DATE_SUB(NOW(), INTERVAL 24 hour);`;
            pool.getConnection(async (err, connection) => {
              if (err) {
                console.log(err);
                return rejects(err);
              }
              await connection.query(sqlquery, async (error, results, fields) => {
                connection.release();
                if (error) {
                  console.error("Error querying MySQL:", error);
                  return rejects(error);
                }
              
                const timestamp = Date.now();
                const today = new Date(timestamp);
                const yyyy = today.getFullYear();
                const mm = today.getMonth() + 1;
                const dd = today.getDate();
                const filename = `PAID_${mm}_${dd}_${yyyy}`;
                console.log("excel_name",filename)
                console.log(filename);
                if (results.length > 0) {
                  const result = await results;
                  const columnNames = await fields.map((field) => field.name);
                  let ViewDataArray = {
                    filename: filename,
                    results: result,
                    columnNames: columnNames
                  };
                  await GenrateExcel(ViewDataArray);
                  return resolve({
                    filename: filename,
                    results: result,
                    columnNames: columnNames,
                  });
                } else {
                  const logmessege = `No Data Found !`;
                  resolve(logmessege);
                }
              });
            });
  });
}


async function FeatchPaymnetDataHRB() {
  return new Promise(async (resolve, rejects) => {
      const timestamp = Date.now();
    const date = new Date(timestamp);
    let sqlquery = `select sale_id as sale_id, product_id as product_id, product_name as product_name, payment_amount as payment_amount, payer_first_name as first_name, payer_last_name as last_name, payer_phone as phone, payer_email as email, payer_address_city as city, payer_address_state as state, payer_address_zip as zipcode, DATE_FORMAT(payment_time, '%m/%d/%Y %H:%i') as date, receiver as receiver, role as role, member_id as member_id, amount as amount, paid as paid, transaction_id as transaction_id,broker_id as institution_code from ETX_Prod where payment_amount = 0 and payment_time >= DATE_SUB(NOW(), INTERVAL 24 hour);`;
            pool.getConnection(async (err, connection) => {
              if (err) {
                console.log(err);
                return rejects(err);
              }
              await connection.query(sqlquery, async (error, results, fields) => {
                connection.release();
                if (error) {
                  console.error("Error querying MySQL:", error);
                  return rejects(error);
                }
              
                const timestamp = Date.now();
                const today = new Date(timestamp);
                const yyyy = today.getFullYear();
                const mm = today.getMonth() + 1;
                const dd = today.getDate();
                const filename = `HRB_${mm}_${dd}_${yyyy}`;
                console.log("excel_name",filename)
                console.log(filename);
                if (results.length > 0) {
                  const result = await results;
                  const columnNames = await fields.map((field) => field.name);
                  let ViewDataArray = {
                    filename: filename,
                    results: result,
                    columnNames: columnNames
                  };
                  await GenrateExcel(ViewDataArray);
                  return resolve({
                    filename: filename,
                    results: result,
                    columnNames: columnNames,
                  });
                } else {
                  const logmessege = `No Data Found !`;
                  resolve(logmessege);
                }
              });
            });
  });
}

async function FeatchPaymnetDataSummary() {
  return new Promise(async (resolve, rejects) => {
      const timestamp = Date.now();
    const date = new Date(timestamp);
    let sqlquery = `select payer_first_name as first_name, payer_last_name as last_name,
                    payer_email as emails, payer_phone as phone , broker_id as institution_code
                    from ETX_Prod where payment_time >= DATE_SUB(NOW(), INTERVAL 24 Hour);`;
            pool.getConnection(async (err, connection) => {
              if (err) {
                console.log(err);
                return rejects(err);
              }
              await connection.query(sqlquery, async (error, results, fields) => {
                connection.release();
                if (error) {
                  console.error("Error querying MySQL:", error);
                  return rejects(error);
                }
              
                const timestamp = Date.now();
                const today = new Date(timestamp);
                const yyyy = today.getFullYear();
                const mm = today.getMonth() + 1;
                const dd = today.getDate();
                //Xullu Customer Accounts 7.16.2024
                const filename = `Xullu_Customer_Accounts_${mm}.${dd}.${yyyy}`;
                console.log("excel_name",filename)
                console.log(filename);
                if (results.length > 0) {
                  const result = await results;
                  const columnNames = await fields.map((field) => field.name);
                  let ViewDataArray = {
                    filename: filename,
                    results: result,
                    columnNames: columnNames
                  };
                  await GenrateExcel(ViewDataArray);
                  return resolve({
                    filename: filename,
                    results: result,
                    columnNames: columnNames,
                  });
                } else {
                  const logmessege = `No Data Found !`;
                  resolve(logmessege);
                }
              });
            });
  });
}
module.exports = {
  paymentresponse: module.exports.paymentresponse,
  GenrateExcel,
  FeatchPaymnetData
};
