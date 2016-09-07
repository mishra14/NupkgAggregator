﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
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
        private string _resultsCsvPath;
        private string _uniqueIdCsvPath;
        private string _allPackagesCsvPath;
        private string _packageCollectionFilePath;
        private object _packageCollectionLock;

        private Dictionary<string, Dictionary<string, List<string>>> _packageCollection;
        private Dictionary<string, Dictionary<string, long>> _downloadCounts;
        private Dictionary<string, long> _downloadCountsOverIds;
        private HashSet<string> _unlistedPackageIds;

        public NupkgPs1Parser(string inputPath, string outputPath, string logPath)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _resultsCsvPath = Path.Combine(logPath, @"results.csv");
            _uniqueIdCsvPath = Path.Combine(logPath, @"uniqueId.csv");
            _allPackagesCsvPath = Path.Combine(logPath, @"allPackages.csv");
            _packageCollectionFilePath = Path.Combine(logPath, @"packageCollection.txt");

            _packageCollectionLock = new object();

            _packageCollection = new Dictionary<string, Dictionary<string, List<string>>>();
            _downloadCounts = new Dictionary<string, Dictionary<string, long>>();
            _downloadCountsOverIds = new Dictionary<string, long>();
            _unlistedPackageIds = new HashSet<string>();

            createFile(_resultsCsvPath);
            createFile(_uniqueIdCsvPath);
            createFile(_allPackagesCsvPath);
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
            if (nupkgParser.packageCollectionExists())
            {
                nupkgParser.deserializePackageCollection();
            }
            else
            {
                nupkgParser.enumerateFiles();
                nupkgParser.serializePackageCollection();
            }

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
                ParallelOptions ops = new ParallelOptions { MaxDegreeOfParallelism = 8 };
                Parallel.ForEach(inputFiles, ops, file =>
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

        private void serializePackageCollection()
        {
            createFile(_packageCollectionFilePath);
            var count = _packageCollection.Count();
            var json = JsonConvert.SerializeObject(_packageCollection, Formatting.Indented);
            //write to _packageCollectionFilePath
            File.WriteAllText(_packageCollectionFilePath, json);
        }

        private void deserializePackageCollection()
        {
            _packageCollection.Clear();
            //read into _packageCollection
            var json = File.ReadAllText(_packageCollectionFilePath);
            _packageCollection = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, List<string>>>>(json);
        }

        private bool packageCollectionExists()
        {
            return File.Exists(_packageCollectionFilePath);
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
                    using (var packageReader = new PackageArchiveReader(archive))
                    {
                        var packageIdentity = packageReader.GetIdentity();
                        //var v3Path = createV3Folders(packageIdentity);
                        //var newPath = Path.Combine(v3Path, Path.GetFileName(path));

                        // Copy nupkg into v3 style structure
                        //File.Copy(path, newPath);

                        // Read nupkg for ps1's and extract the nupkgs into the same folder as nupkg

                        foreach (var ps1FilePath in ps1Files)
                        {
                            //var filePath = Path.Combine(v3Path, ps1FilePath);
                            //createDirectory(Directory.GetParent(filePath).FullName);
                            using (var fileStream = archive.GetEntry(ps1FilePath).Open())
                            {
                                var md5Hash = calculateHashFromStream(fileStream);
                                addToPackageCollection(packageIdentity, ps1FilePath, md5Hash);
                            }
                        }
                    }
                }
            }
        }

        private void addToPackageCollection(PackageIdentity packageIdentity, string fileName, string md5Hash)
        {
            string id = packageIdentity.Id;
            string version = packageIdentity.Version.ToString();
            var fileKey = string.Concat(md5Hash, "_", fileName);
            lock (_packageCollectionLock)
            {
                if (_packageCollection.ContainsKey(id))
                {
                    if (!_packageCollection[id].ContainsKey(fileKey))
                    {
                        _packageCollection[id][fileKey] = new List<string>();
                    }
                    _packageCollection[id][fileKey].Add(version);
                }
                else
                {
                    _packageCollection[id] = new Dictionary<string, List<string>>();
                    _packageCollection[id][fileKey] = new List<string>();
                    _packageCollection[id][fileKey].Add(version);
                }
            }
        }

        private string calculateHash(string path)
        {
            using (var md5 = MD5.Create())
            {
                using (var stream = File.OpenRead(path))
                {
                    var md5ByteArray = md5.ComputeHash(stream);
                    var md5StringBuilder = new StringBuilder();
                    for (int i = 0; i < md5ByteArray.Length; i++)
                    {
                        md5StringBuilder.Append(md5ByteArray[i].ToString("x2"));
                    }
                    return md5StringBuilder.ToString();
                }
            }
        }

        private string calculateHashFromStream(Stream fileStream)
        {
            using (var md5 = MD5.Create())
            {
                var md5ByteArray = md5.ComputeHash(fileStream);
                var md5StringBuilder = new StringBuilder();
                for (int i = 0; i < md5ByteArray.Length; i++)
                {
                    md5StringBuilder.Append(md5ByteArray[i].ToString("x2"));
                }
                return md5StringBuilder.ToString();
            }
        }

        private void populateResultsCsv()
        {
            using (StreamWriter w = File.AppendText(_resultsCsvPath))
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
            using (StreamWriter w = File.AppendText(_uniqueIdCsvPath))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    var idDownloadCount = getDownloadCountFromIdCache(id);
                    w.WriteLine(id + "," + idDownloadCount);
                }
            }
        }

        private void populateAllPackageCsv()
        {
            var seen = new HashSet<string>();
            using (StreamWriter w = File.AppendText(_allPackagesCsvPath))
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
            Console.WriteLine("Priming download count cache");
            long count = 0;
            foreach (var id in _packageCollection.Keys)
            {
                count++;
                if (count % 1000 == 0)
                {
                    Console.WriteLine("Done with " + count + " ids out of " + _packageCollection.Count);
                }
                primeDownloadCountsCache(id);
            }
            Console.WriteLine("Done with Priming download count cache");
        }

        private void primeDownloadCountsCache(string id)
        {
            if (!_downloadCounts.ContainsKey(id) && !_unlistedPackageIds.Contains(id))
            {
                var jsonResponse = queryNuGetForDownloadData(id);
                populateDownloadCounts(id, jsonResponse);
            }
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
            primeDownloadCountsCache(id);
            return getDownloadCountFromCache(id, version);
        }

        private long getDownloadCount(string id)
        {
            primeDownloadCountsCache(id);
            return getDownloadCountFromIdCache(id);
        }

        private void populateDownloadCounts(string id, JObject nugetQueryJson)
        {
            var dataObjectArray = nugetQueryJson["data"].Value<JArray>();
            if (dataObjectArray.Count > 0)
            {
                var dataObject = dataObjectArray[0];
                var versionDataList = dataObject["versions"]?.Values<JObject>();
                if (versionDataList != null)
                {
                    var versionCounts = new Dictionary<string, long>();
                    long totalCount = 0;
                    foreach (var versionData in versionDataList)
                    {
                        var version = versionData["version"].Value<string>();
                        var downloadCount = versionData["downloads"].Value<long>();
                        versionCounts[version] = downloadCount;
                        totalCount += downloadCount;
                    }
                    _downloadCounts[id] = versionCounts;
                    _downloadCountsOverIds[id] = totalCount;
                }
            }
            else
            {
                _unlistedPackageIds.Add(id);
            }
        }

        private long getDownloadCountFromCache(string id, string version)
        {
            return (_downloadCounts.ContainsKey(id) && _downloadCounts[id].ContainsKey(version)) ? _downloadCounts[id][version] : -1;
        }

        private long getDownloadCountFromIdCache(string id)
        {
            return (_downloadCountsOverIds.ContainsKey(id)) ? _downloadCountsOverIds[id] : -1;
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
            using (StreamWriter w = File.AppendText(_resultsCsvPath))
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