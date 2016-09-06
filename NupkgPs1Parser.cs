using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NuGet.Packaging;
using NuGet.Packaging.Core;

namespace Nuget.NupkgParser
{
    internal class NupkgPs1Parser
    {
        private const string _downloadCountUrl = @"https://api-v2v3search-0.nuget.org/query?q=packageid:";

        private string _inputPath;
        private string _outputPath;
        private string _resultsCsv;
        private string _uniqueIdCsv;
        private string _allPackagesCsv;

        private ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentQueue<string>>> _packageCollection;
        private ConcurrentDictionary<string, byte> _fileHashes;
        private Dictionary<string, Dictionary<string, long>> _downloadCounts;

        public NupkgPs1Parser(string inputPath, string outputPath, string logPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _resultsCsv = Path.Combine(logPath, @"results.csv");
            _uniqueIdCsv = Path.Combine(logPath, @"uniqueId.csv");
            _allPackagesCsv = Path.Combine(logPath, @"allPackages.csv");
            _packageCollection = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentQueue<string>>>();
            _fileHashes = new ConcurrentDictionary<string, byte>();
            _downloadCounts = new Dictionary<string, Dictionary<string, long>>();

            createFile(_resultsCsv);
            createFile(_uniqueIdCsv);
            createFile(_allPackagesCsv);
        }

        public void createFile(string path)
        {
            if (!File.Exists(path))
            {
                File.Create(path).Close();
            }
        }

        private static void Main(string[] args)
        {
            NupkgPs1Parser nupkgParser = new NupkgPs1Parser(inputPath: @"F:\MirrorPackages", outputPath: @"\\scratch2\scratch\anmishr\MirrorPackages_v3",
                                                            logPath: @"F:\ProcessedPackages");
            nupkgParser.enumerateFiles();
            Console.WriteLine("Writting results into log");
            nupkgParser.primeDownloadCountsCache();
            nupkgParser.populateResultsCsv();
            nupkgParser.populateUniqueIdCsv();
            nupkgParser.populateAllPackageCsv();
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
                Parallel.ForEach(inputFiles, file =>
                {
                    count++;
                    if (count % 10000 == 0)
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
            using (StreamWriter w = File.AppendText(_resultsCsv))
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

        private void populateUniqueIdCsv()
        {
            using (StreamWriter w = File.AppendText(_uniqueIdCsv))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    w.WriteLine(id);
                }
            }
        }

        private void populateAllPackageCsv()
        {
            var seen = new HashSet<string>();
            using (StreamWriter w = File.AppendText(_allPackagesCsv))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileName in _packageCollection[id].Keys)
                    {
                        foreach (var version in _packageCollection[id][fileName])
                        {
                            var count = getDownloadCount(id, version);
                            var dataString = string.Concat(id + "," + version + "," + count);
                            if (!seen.Contains(dataString))
                            {
                                seen.Add(dataString);
                                w.WriteLine(dataString);
                            }
                        }
                    }
                }
            }
        }

        private void primeDownloadCountsCache()
        {
            foreach (var id in _packageCollection.Keys)
            {
                primeDownloadCountsCache(id);
            }
        }

        private void primeDownloadCountsCache(string id)
        {
            var jsonResponse = queryNuGetForDownloadData(id);
            populateDownloadCounts(id, jsonResponse);
        }

        private JObject queryNuGetForDownloadData(string id)
        {
            using (var wc = new WebClient())
            {
                var jsonResponse = JObject.Parse(wc.DownloadString(string.Concat(_downloadCountUrl, id, "&prerelease=true")));
                //var json = JObject.Parse("{\"@context\":{\"@vocab\":\"http://schema.nuget.org/schema#\",\"@base\":\"https://api.nuget.org/v3/registration0/\"},\"totalHits\":1,\"lastReopen\":\"2016-09-06T19:35:04.7078505Z\",\"index\":\"v3-lucene0-v2v3-20160725\",\"data\":[{\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/index.json\",\"@type\":\"Package\",\"registration\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/index.json\",\"id\":\"Newtonsoft.Json\",\"version\":\"9.0.1\",\"description\":\"Json.NET is a popular high-performance JSON framework for .NET\",\"summary\":\"\",\"title\":\"Json.NET\",\"iconUrl\":\"http://www.newtonsoft.com/content/images/nugeticon.png\",\"licenseUrl\":\"https://raw.github.com/JamesNK/Newtonsoft.Json/master/LICENSE.md\",\"projectUrl\":\"http://www.newtonsoft.com/json\",\"tags\":[\"json\"],\"authors\":[\"James Newton-King\"],\"totalDownloads\":35972382,\"versions\":[{\"version\":\"3.5.8\",\"downloads\":31842,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/3.5.8.json\"},{\"version\":\"4.0.1\",\"downloads\":24634,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.1.json\"},{\"version\":\"4.0.2\",\"downloads\":51515,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.2.json\"},{\"version\":\"4.0.3\",\"downloads\":29743,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.3.json\"},{\"version\":\"4.0.4\",\"downloads\":28415,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.4.json\"},{\"version\":\"4.0.5\",\"downloads\":73106,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.5.json\"},{\"version\":\"4.0.6\",\"downloads\":12694,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.6.json\"},{\"version\":\"4.0.7\",\"downloads\":256623,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.7.json\"},{\"version\":\"4.0.8\",\"downloads\":222551,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.8.json\"},{\"version\":\"4.5.1\",\"downloads\":177144,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.1.json\"},{\"version\":\"4.5.2\",\"downloads\":12432,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.2.json\"},{\"version\":\"4.5.3\",\"downloads\":24283,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.3.json\"},{\"version\":\"4.5.4\",\"downloads\":51220,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.4.json\"},{\"version\":\"4.5.5\",\"downloads\":63726,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.5.json\"},{\"version\":\"4.5.6\",\"downloads\":879895,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.6.json\"},{\"version\":\"4.5.7\",\"downloads\":223289,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.7.json\"},{\"version\":\"4.5.8\",\"downloads\":211536,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.8.json\"},{\"version\":\"4.5.9\",\"downloads\":170790,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.9.json\"},{\"version\":\"4.5.10\",\"downloads\":278682,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.10.json\"},{\"version\":\"4.5.11\",\"downloads\":2579161,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.11.json\"},{\"version\":\"5.0.1\",\"downloads\":350752,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.1.json\"},{\"version\":\"5.0.2\",\"downloads\":243064,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.2.json\"},{\"version\":\"5.0.3\",\"downloads\":260181,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.3.json\"},{\"version\":\"5.0.4\",\"downloads\":734620,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.4.json\"},{\"version\":\"5.0.5\",\"downloads\":443202,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.5.json\"},{\"version\":\"5.0.6\",\"downloads\":1834875,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.6.json\"},{\"version\":\"5.0.7\",\"downloads\":415827,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.7.json\"},{\"version\":\"5.0.8\",\"downloads\":1964082,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.8.json\"},{\"version\":\"6.0.1\",\"downloads\":1053770,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.1.json\"},{\"version\":\"6.0.2\",\"downloads\":611365,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.2.json\"},{\"version\":\"6.0.3\",\"downloads\":1405283,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.3.json\"},{\"version\":\"6.0.4\",\"downloads\":3573169,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.4.json\"},{\"version\":\"6.0.5\",\"downloads\":1356971,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.5.json\"},{\"version\":\"6.0.6\",\"downloads\":2545024,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.6.json\"},{\"version\":\"6.0.7\",\"downloads\":582617,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.7.json\"},{\"version\":\"6.0.8\",\"downloads\":3637795,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.8.json\"},{\"version\":\"7.0.1\",\"downloads\":4053031,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/7.0.1.json\"},{\"version\":\"8.0.1\",\"downloads\":469135,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.1.json\"},{\"version\":\"8.0.2\",\"downloads\":1707785,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.2.json\"},{\"version\":\"8.0.3\",\"downloads\":1994315,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.3.json\"},{\"version\":\"9.0.1\",\"downloads\":1010277,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/9.0.1.json\"}]}]}");
                return jsonResponse;
            }
        }

        private long getDownloadCount(string id, string version)
        {
            if (!_downloadCounts.ContainsKey(id))
            {
                primeDownloadCountsCache(id);
            }
            return getDownloadCountFromCache(id, version);
        }

        private void populateDownloadCounts(string id, JObject nugetQueryJson)
        {
            var dataObject = nugetQueryJson["data"].Value<JArray>()[0];
            var versionDataList = dataObject["versions"].Values<JObject>();
            var versionCounts = new Dictionary<string, long>();
            foreach (var versionData in versionDataList)
            {
                var version = versionData["version"].Value<string>();
                var downloadCount = versionData["downloads"].Value<long>();
                versionCounts[version] = downloadCount;
            }
            _downloadCounts[id] = versionCounts;
        }

        private long getDownloadCountFromCache(string id, string version)
        {
            return (_downloadCounts.ContainsKey(id) && _downloadCounts[id].ContainsKey(version)) ? _downloadCounts[id][version] : -1;
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
            using (StreamWriter w = File.AppendText(_resultsCsv))
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