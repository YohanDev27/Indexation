const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { performance } = require('perf_hooks');
const { readFileSync, writeFileSync, existsSync } = require('fs');
const mongoose = require('mongoose');

async function connectToDBMongoose() {
  const uri = 'mongodb://127.0.0.1:27017/index2';
  try {
      await mongoose.connect(uri);
      console.log('Connected to MongoDB with Mongoose');
  } catch (error) {
      console.error('Error connecting to MongoDB with Mongoose:', error);
      throw error;
  }
}


let isPaused = false;

const etablissementSchema = new mongoose.Schema({
  siren: { type: String, required: true },
  nic: { type: String, required: true },
  siret: { type: String, required: true },
  dateCreationEtablissement: { type: Date, required: true },
  dateDernierTraitementEtablissement: { type: Date, required: true },
  typeVoieEtablissement: { type: String, required: true },
  libelleVoieEtablissement: { type: String, required: true },
  codePostalEtablissement: { type: String, required: true },
  libelleCommuneEtablissement: { type: String, required: true },
  codeCommuneEtablissement: { type: String, required: true },
  dateDebut: { type: Date, required: true },
  etatAdministratifEtablissement: { type: String, required: true },
  
  statutDiffusionEtablissement: String,
  trancheEffectifsEtablissement: String,
  anneeEffectifsEtablissement: String,
  activitePrincipaleRegistreMetiers: String,
  etablissementSiege: String,
  nombrePeriodesEtablissement: String,
  complementAdresseEtablissement: String,
  numeroVoieEtablissement: String,
  indiceRepetitionEtablissement: String,
  codeCedexEtablissement: String,
  libelleCedexEtablissement: String,
  codePaysEtrangerEtablissement: String,
  libellePaysEtrangerEtablissement: String,
  distributionSpecialeEtablissement: String
});

const TP = mongoose.model('TP', etablissementSchema);

function pauseProcess() {
  isPaused = true;
  console.log('Processus en pause...');
}

function resumeProcess() {
  isPaused = false;
  console.log('Processus repris.');
}

process.on('SIGINT', () => {
  isPaused ? resumeProcess() : pauseProcess();
});

async function processFile(filePath) {
  await connectToDBMongoose();
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let bulkOps = [];
  let headers;
  let startTime = performance.now();

  for await (const line of rl) {
    if (isPaused) await new Promise(resolve => setTimeout(resolve, 1000));

    if (!headers) {
      headers = line.split(',');
      continue;
    }

    const recordData = line.split(',').reduce((acc, value, idx) => {
      acc[headers[idx]] = value.trim();
      return acc;
    }, {});

   
    const record = new TP(recordData);

    try {
      await record.validate(); 
      bulkOps.push({ insertOne: { document: record.toObject() } }); 
    } catch (error) {
    }

    if (bulkOps.length >= 1000) {
      await TP.bulkWrite(bulkOps); 
      bulkOps = [];
    }
  }

  if (bulkOps.length) {
    await TP.bulkWrite(bulkOps); 
  }

  let endTime = performance.now();
  return (endTime - startTime) / 1000;  
}



async function processAllFiles(folderPath, workerId, totalWorkers, totalFiles) {
  let globalStartTime = performance.now();

  workerId = parseInt(workerId, 10);
  const filesPerWorker = Math.ceil(totalFiles / totalWorkers);
  const startFile = (workerId - 1) * filesPerWorker + 1;
  const endFile = Math.min(workerId * filesPerWorker, totalFiles);
  let workerTime = 0;

  for (let i = startFile; i <= endFile; i++) {
    if (isPaused) await new Promise(resolve => setTimeout(resolve, 1000));

    const fileName = `Decompressoutput_${i}.csv`;
    const filePath = path.join(folderPath, fileName);
    try {
      let fileTime = await processFile(filePath);
      workerTime += fileTime;
      console.log(`Worker ${workerId}: Le fichier ${path.basename(filePath)} a été traité en ${fileTime.toFixed(2)} s.`);
    } catch (err) {
      console.error(`Worker ${workerId}: Erreur lors du traitement du fichier ${path.basename(filePath)}: ${err}`);
    }
  }

  let globalEndTime = performance.now();
  console.log(`Temps total d'indexation par le worker ${workerId}: ${(globalEndTime - globalStartTime) / 1000} s`);

  const timesPath = 'times.json';
  let timesData = {};
  if (existsSync(timesPath)) {
    timesData = JSON.parse(readFileSync(timesPath, 'utf8'));
  }
  timesData[workerId] = workerTime;
  writeFileSync(timesPath, JSON.stringify(timesData, null, 2));

  if (Object.keys(timesData).length === totalWorkers) {
    let totalTime = Object.values(timesData).reduce((acc, time) => acc + time, 0);
    console.log(`Temps total d'indexation de tous les workers: ${totalTime.toFixed(2)} s`);
  }
}

const workerId = process.env.WORKER_ID;
const totalWorkers = process.env.TOTAL_WORKERS || 1;
const folderPath = 'G:\\Intech\\S10\\bigData\\Decompress';
const totalFiles = 73;
processAllFiles(folderPath, workerId, totalWorkers, totalFiles);
