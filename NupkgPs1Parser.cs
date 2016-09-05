using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NuGet.Packaging;
using NuGet.Packaging.Core;

namespace Nuget.NupkgParser
{
    internal class NupkgPs1Parser
    {
        private string _inputPath;
        private string _outputPath;
        private string _logFile;

        private ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentQueue<string>>> _packageCollection;

        public NupkgPs1Parser(string inputPath, string outputPath, string logFile, string errorLogFile)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _logFile = logFile;
            _packageCollection = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentQueue<string>>>();
            if (!File.Exists(_logFile))
            {
                File.Create(_logFile).Close();
            }
        }

        private static void Main(string[] args)
        {
            NupkgPs1Parser nupkgParser = new NupkgPs1Parser(@"F:\MirrorPackages", @"F:\ProcessedPackages", @"F:\ProcessedPackages\log.csv", @"F:\ProcessedPackages\errors.txt");
            nupkgParser.enumerateFiles();
            Console.WriteLine("Writting results into log");
            nupkgParser.populateResultsCsv();
        }

        private void enumerateFiles()
        {
            try
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();

                var inputFiles = Directory.EnumerateFiles(_inputPath).ToArray();

                long count = 0;
                long errorCount = 0;
                ParallelOptions ops = new ParallelOptions() { MaxDegreeOfParallelism = 16 };
                Parallel.ForEach(inputFiles, file =>
                {
                    count++;
                    if (count % 5000 == 0)
                    {
                        Console.WriteLine("Done with " + count + " packages with " + errorCount + " errors");
                        double percentDone = (double)count / (double)inputFiles.Length;
                        var timeLeft = ((timer.Elapsed.TotalSeconds / count) * inputFiles.Length) - timer.Elapsed.TotalSeconds;
                        var dateEnd = DateTime.Now.AddSeconds(timeLeft);
                        var timeLeftSpan = TimeSpan.FromSeconds(timeLeft);
                        Console.WriteLine($"Done in {timeLeftSpan} at {dateEnd}");
                    }
                    try
                    {
                        processArchive(file);
                    }
                    catch (Exception ex)
                    {
                        errorCount++;
                        Console.WriteLine("Exception while parsing " + file);
                        Console.WriteLine(ex.Message);
                        logError(Path.GetFileName(file), Path.Combine(_outputPath, @"error_" + Thread.CurrentThread.ManagedThreadId + ".txt"));
                        var curroptFilePath = Path.Combine(@"f:\CurroptPackages", Path.GetFileName(file));

                        File.Move(file, curroptFilePath);
                    }
                });
                Console.WriteLine("Done with all the " + count + " packages");
            }
            catch (AggregateException ae)
            {
                // This is where you can choose which exceptions to handle.
                foreach (var ex in ae.InnerExceptions)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private void processArchive(string path)
        {
            using (var archive = ZipFile.Open(path, ZipArchiveMode.Read))
            {
                var ps1Files = archive.Entries
                    .Where(e => e.FullName.EndsWith(".ps1", StringComparison.OrdinalIgnoreCase))
                    .Select(e => e.FullName)
                    .ToArray();

                if (ps1Files.Length > 0)
                {
                    // Create V3 style location
                    PackageArchiveReader packageReader = new PackageArchiveReader(archive);
                    var packageIdentity = packageReader.GetIdentity();
                    //var v3Path = createV3Folders(packageIdentity);
                    //var newPath = Path.Combine(v3Path, Path.GetFileName(path));

                    // Copy nupkg into v3 style structure
                    //File.Copy(path, newPath);

                    // Read nupkg for ps1's and extract the nupkgs into the same folder as nupkg

                    foreach (var ps1FilePath in ps1Files)
                    {
                        addToPackageCollection(packageIdentity, ps1FilePath);
                        //var filePath = Path.Combine(v3Path, entry.FullName);
                        //createDirectory(Directory.GetParent(filePath).FullName);
                        //entry.ExtractToFile(filePath);

                        //log(string.Concat(packageIdentity.Id, " ", packageIdentity.Version, " ", entry.Name));
                    }
                }
            }
        }

        private void addToPackageCollection(PackageIdentity packageIdentity, string fileName)
        {
            string id = packageIdentity.Id;
            string version = packageIdentity.Version.ToString();
            if (_packageCollection.ContainsKey(id))
            {
                if (!_packageCollection[id].ContainsKey(fileName))
                {
                    _packageCollection[id][fileName] = new ConcurrentQueue<string>();
                }
                _packageCollection[id][fileName].Enqueue(version);
            }
            else
            {
                _packageCollection[id] = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
                _packageCollection[id][fileName] = new ConcurrentQueue<string>();
                _packageCollection[id][fileName].Enqueue(version);
            }
        }

        private void populateResultsCsv()
        {
            using (StreamWriter w = File.AppendText(_logFile))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileName in _packageCollection[id].Keys)
                    {
                        var x = _packageCollection[id][fileName];
                        var versionList = _packageCollection[id][fileName].ToArray();
                        var delimiter = " ";
                        var versionString = versionList.Aggregate((i, j) => i + delimiter + j);
                        w.WriteLine(string.Concat(id, ",", fileName, ",", versionString));
                    }
                }
            }
        }

        private string createV3Folders(PackageIdentity packageIdentity)
        {
            var id = packageIdentity.Id;
            var version = packageIdentity.Version;
            var versionDer = (Path.Combine(_outputPath, id, version.ToString()));
            createDirectory(versionDer);
            return versionDer;
        }

        private void createDirectory(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
        }

        private void log(string line)
        {
            using (StreamWriter w = File.AppendText(_logFile))
            {
                w.WriteLine(line);
            }
        }

        private void logError(string line, string errorFilePath)
        {
            if (!File.Exists(errorFilePath))
            {
                File.Create(errorFilePath).Close();
            }
            using (StreamWriter w = File.AppendText(errorFilePath))
            {
                w.WriteLine(line);
            }
        }
    }
}