const fs = require('fs');
const path = require('path');
const readline = require('readline');

function splitCSV(inputFile, outputFolder, chunkSize) {
    if (!fs.existsSync(outputFolder)) {
        fs.mkdirSync(outputFolder, { recursive: true });
    }

    let inputStream = fs.createReadStream(inputFile);
    let lineReader = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity
    });

    let chunkCount = 1;
    let lines = [];
    
    lineReader.on('line', (line) => {
        lines.push(line);

        if (lines.length >= chunkSize) {
            writeChunkToFile(lines, outputFolder, chunkCount++);
            lines = [];
        }
    });

    lineReader.on('close', () => {
        if (lines.length > 0) {
            writeChunkToFile(lines, outputFolder, chunkCount);
        }
        console.log('Fichier CSV divisé en parties avec succès.');
    });
}

function writeChunkToFile(lines, outputFolder, chunkCount) {
    let outputFile = path.join(outputFolder, `part_${chunkCount}.csv`);
    fs.writeFileSync(outputFile, lines.join('\n'));
}
const inputFile = 'G:/Intech/S10/bigData/StockEtablissement_utf8/StockEtablissement_utf8.csv';
const outputFolder = 'G:/Intech/S10/bigData/Decompress';
const chunkSize = 500000;

splitCSV(inputFile, outputFolder, chunkSize);