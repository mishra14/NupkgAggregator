using System;
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
using NuGet.Versioning;

namespace Nuget.NupkgParser
{
    internal class NupkgPs1Parser
    {
        private const string _downloadCountUrl = @"https://api-v2v3search-0.nuget.org/query?q=packageid:";

        private string _inputPath;
        private string _outputPath;
        private string _resultsCsvPath;
        private string _resultsLatestCsvPath;
        private string _uniqueIdCsvPath;
        private string _allPackagesCsvPath;
        private string _packageCollectionFilePath;
        private string _downloadCountsFilePath;
        private string _downloadCountsOverIdFilePath;
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
            _resultsLatestCsvPath = Path.Combine(logPath, @"resultsLatestversion.csv");
            _uniqueIdCsvPath = Path.Combine(logPath, @"uniqueId.csv");
            _allPackagesCsvPath = Path.Combine(logPath, @"allPackages.csv");
            _packageCollectionFilePath = Path.Combine(logPath, @"packageCollection.txt");
            _downloadCountsFilePath = Path.Combine(logPath, @"downloadsCounts.txt");
            _downloadCountsOverIdFilePath = Path.Combine(logPath, @"downloadsCountsOverId.txt");

            _packageCollectionLock = new object();

            _packageCollection = new Dictionary<string, Dictionary<string, List<string>>>();
            _downloadCounts = new Dictionary<string, Dictionary<string, long>>();
            _downloadCountsOverIds = new Dictionary<string, long>();
            _unlistedPackageIds = new HashSet<string>();

            CreateFile(_resultsCsvPath);
            CreateFile(_uniqueIdCsvPath);
            CreateFile(_allPackagesCsvPath);
            CreateDirectory(_outputPath);
            CreateDirectory(logPath);
        }

        public void CreateFile(string path)
        {
            var dir = Path.GetDirectoryName(path);
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
            if (!File.Exists(path))
            {
                File.Create(path).Close();
            }
        }

        private void CreateDirectory(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
        }

        private static void Main(string[] args)
        {
            NupkgPs1Parser nupkgParser = new NupkgPs1Parser(inputPath: @"F:\NupkgParser\MirrorPackages", outputPath: @"F:\NupkgParser\MirrorPackages_deduped",
                                                            logPath: @"F:\NupkgParser\ProcessedLogs");
            Console.WriteLine("Populating packageCollection");
            if (nupkgParser.PackageCollectionExists())
            {
                nupkgParser.DeserializePackageCollection();
            }
            else
            {
                nupkgParser.EnumerateFiles();
                nupkgParser.SerializePackageCollection();
            }

            Console.WriteLine("Populating download counts");
            if (nupkgParser.DownloadCountsExist())
            {
                nupkgParser.DeserializeDownloadCounts();
            }
            else
            {
                nupkgParser.PrimeDownloadCountsCache();
                nupkgParser.SerializeDownloadCounts();
            }

            Console.WriteLine("Writting results into log");
            nupkgParser.PopulateResultsCsv();
            nupkgParser.PopulateUniqueIdCsv();
            //nupkgParser.populateLatestResultCsv();
            //nupkgParser.populateAllPackageCsv();
        }

        private void EnumerateFiles()
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
                        ProcessArchiveForContentFile(file);
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

        private void SerializePackageCollection()
        {
            CreateFile(_packageCollectionFilePath);
            var json = JsonConvert.SerializeObject(_packageCollection, Formatting.Indented);
            //write to _packageCollectionFilePath
            File.WriteAllText(_packageCollectionFilePath, json);
        }

        private void DeserializePackageCollection()
        {
            _packageCollection.Clear();
            //read into _packageCollection
            var json = File.ReadAllText(_packageCollectionFilePath);
            _packageCollection = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, List<string>>>>(json);
        }

        private bool PackageCollectionExists()
        {
            return File.Exists(_packageCollectionFilePath);
        }

        private void SerializeDownloadCounts()
        {
            CreateFile(_downloadCountsFilePath);
            var json = JsonConvert.SerializeObject(_downloadCounts, Formatting.Indented);
            //write to _packageCollectionFilePath
            File.WriteAllText(_downloadCountsFilePath, json);

            CreateFile(_downloadCountsOverIdFilePath);
            json = JsonConvert.SerializeObject(_downloadCountsOverIds, Formatting.Indented);
            //write to _packageCollectionFilePath
            File.WriteAllText(_downloadCountsOverIdFilePath, json);
        }

        private void DeserializeDownloadCounts()
        {
            _downloadCounts.Clear();
            _downloadCountsOverIds.Clear();
            //read into _packageCollection
            var json = File.ReadAllText(_downloadCountsFilePath);
            _downloadCounts = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, long>>>(json);
            json = File.ReadAllText(_downloadCountsOverIdFilePath);
            _downloadCountsOverIds = JsonConvert.DeserializeObject<Dictionary<string, long>>(json);
        }

        private bool DownloadCountsExist()
        {
            return File.Exists(_downloadCountsFilePath) && File.Exists(_downloadCountsOverIdFilePath);
        }

        private void ProcessArchivePs1(string path)
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
                        //var newPath = Path.Combine(_outputPath, Path.GetFileName(path));

                        // Copy nupkg into v3 style structure
                        //File.Copy(path, newPath);

                        // Read nupkg for ps1's and extract the nupkgs into the same folder as nupkg
                        foreach (var ps1FilePath in ps1Files)
                        {
                            //var filePath = Path.Combine(v3Path, ps1FilePath);
                            //createDirectory(Directory.GetParent(filePath).FullName);
                            using (var fileStream = archive.GetEntry(ps1FilePath).Open())
                            {
                                var md5Hash = CalculateHashFromStream(fileStream);
                                AddToPackageCollection(packageIdentity, ps1FilePath, md5Hash);
                            }
                        }
                    }
                }
            }
        }

        private void ProcessArchiveNuSpec(string path)
        {
            using (var archive = ZipFile.Open(path, ZipArchiveMode.Read))
            {
                var nuspecFiles = archive.Entries
                    .Where(e => e.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
                    .Select(e => e.FullName)
                    .ToArray();

                if (nuspecFiles.Length > 0)
                {
                    using (var packageReader = new PackageArchiveReader(archive))
                    {
                        var packageIdentity = packageReader.GetIdentity();

                        // Read nupkg for ps1's and extract the nupkgs into the same folder as nupkg
                        foreach (var nuspecFile in nuspecFiles)
                        {
                            using (var fileStream = archive.GetEntry(nuspecFile).Open())
                            {
                                // Parse NuSpec to find metadata/contentfiles tag
                                var nuspec = new NuspecReader(fileStream);
                                if (nuspec.GetContentFiles().Count() > 0)
                                {
                                    AddToPackageCollection(packageIdentity, nuspecFile, "");
                                }
                            }
                        }
                    }
                }
            }
        }

        private void ProcessArchiveForPP(string path)
        {
            using (var archive = ZipFile.Open(path, ZipArchiveMode.Read))
            {
                var ppFiles = archive.Entries.Where(e => e.FullName.EndsWith(".pp"));
                if (ppFiles.Count() > 0)
                {
                    using (var packageReader = new PackageArchiveReader(archive))
                    {
                        var packageIdentity = packageReader.GetIdentity();
                        foreach (var ppFile in ppFiles)
                        {
                            AddToPackageCollection(packageIdentity, ppFile.FullName, "");
                        }
                    }
                }
            }
        }

        private void ProcessArchiveForContentFile(string path)
        {
            using (var archive = ZipFile.Open(path, ZipArchiveMode.Read))
            {                
                var contentFiles = archive.Entries
                    .Where(e => e.FullName.StartsWith("contentfiles", StringComparison.OrdinalIgnoreCase));

                if (contentFiles.Count() > 0)
                {
                    using (var packageReader = new PackageArchiveReader(archive))
                    {
                        var packageIdentity = packageReader.GetIdentity();
                        foreach (var contentFile in contentFiles)
                        {
                            AddToPackageCollection(packageIdentity, contentFile.FullName, "");
                        }
                    }
                }
            }
        }

        private void AddToPackageCollection(PackageIdentity packageIdentity, string fileName, string md5Hash)
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

        private string CalculateHash(string path)
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

        private string CalculateHashFromStream(Stream fileStream)
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

        private void PopulateResultsCsv()
        {
            Console.WriteLine("Populating PackageId + File csv");
            using (StreamWriter w = File.AppendText(_resultsCsvPath))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        var versionList = _packageCollection[id][fileKey].ToArray();
                        var delimiter = " ";
                        var versionString = versionList.Aggregate((i, j) => i + delimiter + j);
                        w.WriteLine(string.Concat(id, ",", fileKey, ",", versionString));
                    }
                }
            }
        }

        private void PopulateLatestResultCsv()
        {
            Console.WriteLine("Populating PackageId + File + Latest Version csv");
            using (StreamWriter w = File.AppendText(_resultsLatestCsvPath))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        var latestVersion = _packageCollection[id][fileKey]
                                            .Select(x => new NuGetVersion(x))
                                            .OrderByDescending(version => version)
                                            .Max();
                        // var versionObjectList = versionList.Select(x => new NuGetVersion(x)).ToList();
                        // var latestVersion = versionObjectList.OrderByDescending(version => version).Max();

                        w.WriteLine(string.Concat(id, ",", fileKey, ",", latestVersion.ToNormalizedString()));

                        // Move latest version into deduped location
                        var latestVersionFileName = string.Concat(id, ".", latestVersion.ToNormalizedString(), ".nupkg");
                        if (!File.Exists(Path.Combine(_outputPath, latestVersionFileName)))
                        {
                            File.Copy(Path.Combine(_inputPath, latestVersionFileName), Path.Combine(_outputPath, latestVersionFileName));
                        }
                    }
                }
            }
        }

        private void PopulateUniqueIdCsv()
        {
            Console.WriteLine("Populating Unique PackageId csv");
            using (StreamWriter w = File.AppendText(_uniqueIdCsvPath))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    var idDownloadCount = GetDownloadCountFromIdCache(id);
                    w.WriteLine(id + "," + idDownloadCount);
                }
            }
        }

        private void PopulateAllPackageCsv()
        {
            Console.WriteLine("Populating All Packages csv");
            var seen = new HashSet<string>();
            using (StreamWriter w = File.AppendText(_allPackagesCsvPath))
            {
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        foreach (var version in _packageCollection[id][fileKey])
                        {
                            var count = GetDownloadCount(id, version);
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

        private void PrimeDownloadCountsCache()
        {
            long count = 0;
            foreach (var id in _packageCollection.Keys)
            {
                count++;
                if (count % 1000 == 0)
                {
                    Console.WriteLine("Done with " + count + " ids out of " + _packageCollection.Count);
                }
                PrimeDownloadCountsCache(id);
            }
        }

        private void PrimeDownloadCountsCache(string id)
        {
            if (!_downloadCounts.ContainsKey(id) && !_unlistedPackageIds.Contains(id))
            {
                var jsonResponse = QueryNuGetForDownloadData(id);
                PopulateDownloadCounts(id, jsonResponse);
            }
        }

        private JObject QueryNuGetForDownloadData(string id)
        {
            using (var wc = new WebClient())
            {
                var jsonResponse = JObject.Parse(wc.DownloadString(string.Concat(_downloadCountUrl, id, "&prerelease=true")));
                //var json = JObject.Parse("{\"@context\":{\"@vocab\":\"http://schema.nuget.org/schema#\",\"@base\":\"https://api.nuget.org/v3/registration0/\"},\"totalHits\":1,\"lastReopen\":\"2016-09-06T19:35:04.7078505Z\",\"index\":\"v3-lucene0-v2v3-20160725\",\"data\":[{\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/index.json\",\"@type\":\"Package\",\"registration\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/index.json\",\"id\":\"Newtonsoft.Json\",\"version\":\"9.0.1\",\"description\":\"Json.NET is a popular high-performance JSON framework for .NET\",\"summary\":\"\",\"title\":\"Json.NET\",\"iconUrl\":\"http://www.newtonsoft.com/content/images/nugeticon.png\",\"licenseUrl\":\"https://raw.github.com/JamesNK/Newtonsoft.Json/master/LICENSE.md\",\"projectUrl\":\"http://www.newtonsoft.com/json\",\"tags\":[\"json\"],\"authors\":[\"James Newton-King\"],\"totalDownloads\":35972382,\"versions\":[{\"version\":\"3.5.8\",\"downloads\":31842,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/3.5.8.json\"},{\"version\":\"4.0.1\",\"downloads\":24634,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.1.json\"},{\"version\":\"4.0.2\",\"downloads\":51515,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.2.json\"},{\"version\":\"4.0.3\",\"downloads\":29743,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.3.json\"},{\"version\":\"4.0.4\",\"downloads\":28415,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.4.json\"},{\"version\":\"4.0.5\",\"downloads\":73106,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.5.json\"},{\"version\":\"4.0.6\",\"downloads\":12694,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.6.json\"},{\"version\":\"4.0.7\",\"downloads\":256623,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.7.json\"},{\"version\":\"4.0.8\",\"downloads\":222551,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.0.8.json\"},{\"version\":\"4.5.1\",\"downloads\":177144,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.1.json\"},{\"version\":\"4.5.2\",\"downloads\":12432,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.2.json\"},{\"version\":\"4.5.3\",\"downloads\":24283,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.3.json\"},{\"version\":\"4.5.4\",\"downloads\":51220,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.4.json\"},{\"version\":\"4.5.5\",\"downloads\":63726,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.5.json\"},{\"version\":\"4.5.6\",\"downloads\":879895,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.6.json\"},{\"version\":\"4.5.7\",\"downloads\":223289,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.7.json\"},{\"version\":\"4.5.8\",\"downloads\":211536,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.8.json\"},{\"version\":\"4.5.9\",\"downloads\":170790,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.9.json\"},{\"version\":\"4.5.10\",\"downloads\":278682,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.10.json\"},{\"version\":\"4.5.11\",\"downloads\":2579161,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/4.5.11.json\"},{\"version\":\"5.0.1\",\"downloads\":350752,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.1.json\"},{\"version\":\"5.0.2\",\"downloads\":243064,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.2.json\"},{\"version\":\"5.0.3\",\"downloads\":260181,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.3.json\"},{\"version\":\"5.0.4\",\"downloads\":734620,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.4.json\"},{\"version\":\"5.0.5\",\"downloads\":443202,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.5.json\"},{\"version\":\"5.0.6\",\"downloads\":1834875,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.6.json\"},{\"version\":\"5.0.7\",\"downloads\":415827,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.7.json\"},{\"version\":\"5.0.8\",\"downloads\":1964082,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/5.0.8.json\"},{\"version\":\"6.0.1\",\"downloads\":1053770,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.1.json\"},{\"version\":\"6.0.2\",\"downloads\":611365,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.2.json\"},{\"version\":\"6.0.3\",\"downloads\":1405283,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.3.json\"},{\"version\":\"6.0.4\",\"downloads\":3573169,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.4.json\"},{\"version\":\"6.0.5\",\"downloads\":1356971,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.5.json\"},{\"version\":\"6.0.6\",\"downloads\":2545024,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.6.json\"},{\"version\":\"6.0.7\",\"downloads\":582617,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.7.json\"},{\"version\":\"6.0.8\",\"downloads\":3637795,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/6.0.8.json\"},{\"version\":\"7.0.1\",\"downloads\":4053031,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/7.0.1.json\"},{\"version\":\"8.0.1\",\"downloads\":469135,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.1.json\"},{\"version\":\"8.0.2\",\"downloads\":1707785,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.2.json\"},{\"version\":\"8.0.3\",\"downloads\":1994315,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/8.0.3.json\"},{\"version\":\"9.0.1\",\"downloads\":1010277,\"@id\":\"https://api.nuget.org/v3/registration0/newtonsoft.json/9.0.1.json\"}]}]}");
                return jsonResponse;
            }
        }

        private long GetDownloadCount(string id, string version)
        {
            PrimeDownloadCountsCache(id);
            return GetDownloadCountFromCache(id, version);
        }

        private long getDownloadCount(string id)
        {
            PrimeDownloadCountsCache(id);
            return GetDownloadCountFromIdCache(id);
        }

        private void PopulateDownloadCounts(string id, JObject nugetQueryJson)
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

        private long GetDownloadCountFromCache(string id, string version)
        {
            return (_downloadCounts.ContainsKey(id) && _downloadCounts[id].ContainsKey(version)) ? _downloadCounts[id][version] : -1;
        }

        private long GetDownloadCountFromIdCache(string id)
        {
            return (_downloadCountsOverIds.ContainsKey(id)) ? _downloadCountsOverIds[id] : -1;
        }

        private string CreateV3Folders(PackageIdentity packageIdentity)
        {
            var id = packageIdentity.Id;
            var version = packageIdentity.Version;
            var versionDer = (Path.Combine(_outputPath, id, version.ToString()));
            CreateDirectory(versionDer);
            return versionDer;
        }

        private void Log(string line)
        {
            using (StreamWriter w = File.AppendText(_resultsCsvPath))
            {
                w.WriteLine(line);
            }
        }

        private void LogError(string line, string errorFilePath)
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