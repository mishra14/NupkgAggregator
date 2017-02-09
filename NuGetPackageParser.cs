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
using NuGet.Protocol;
using NuGet.Versioning;

namespace NugetPackages
{
    internal class NuGetPackageParser
    {
        private const string _downloadCountUrl = @"https://api-v2v3search-0.nuget.org/query?q=packageid:";
        private const int _maxThreadCount = 8;

        private string _inputPath;
        private string _outputPath;
        private string _logPath;
        private string _packageCollectionFilePath;
        private string _downloadCountsFilePath;
        private string _downloadCountsOverIdFilePath;
        private object _packageCollectionLock;
        private bool _isInputV3Style;

        private Dictionary<string, Dictionary<string, List<string>>> _packageCollection;
        private Dictionary<string, Dictionary<string, long>> _downloadCounts;
        private Dictionary<string, long> _downloadCountsOverIds;
        private HashSet<string> _unlistedPackageIds;

        public NuGetPackageParser(string inputPath, string outputPath, string logPath, bool isInputV3Style)
        {
            _inputPath = inputPath;
            _outputPath = outputPath;
            _logPath = logPath;
            _packageCollectionFilePath = Path.Combine(logPath, @"packageCollection.txt");
            _downloadCountsFilePath = Path.Combine(logPath, @"downloadsCounts.txt");
            _downloadCountsOverIdFilePath = Path.Combine(logPath, @"downloadsCountsOverId.txt");

            _isInputV3Style = isInputV3Style;

            _packageCollectionLock = new object();

            _packageCollection = new Dictionary<string, Dictionary<string, List<string>>>();
            _downloadCounts = new Dictionary<string, Dictionary<string, long>>();
            _downloadCountsOverIds = new Dictionary<string, long>();
            _unlistedPackageIds = new HashSet<string>();

            CreateDirectory(_outputPath);
            CreateDirectory(_logPath);
        }

        public void CreateFile(string path, bool deleteIfExists)
        {
            var dir = Path.GetDirectoryName(path);
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
            if (File.Exists(path) && deleteIfExists)
            {
                File.Delete(path);
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
            var nugetPackageParser = new NuGetPackageParser(inputPath: @"\\juste-box\nuget", // 
                outputPath: @"F:\NuGetPackageParser\MirrorPackages_deduped",
                logPath: @"F:\NuGetPackageParser\ProcessedLogs",
                isInputV3Style: true);

            Console.WriteLine("Populating packageCollection");
            nugetPackageParser.EnumerateFiles(onlyLatest: false, clearPackageCollectionCache: false);

            Console.WriteLine("Populating download counts");
            nugetPackageParser.GetDownloadsCount(clearDownloadsCache: true);

            Console.WriteLine("Writting results into log");
            //nugetPackageParser.GenerateResults();
            nugetPackageParser.GenerateIdLatestVersionDownloadCountCsv();
        }

        private void EnumerateFiles(bool onlyLatest, bool clearPackageCollectionCache)
        {
            if (!clearPackageCollectionCache && PackageCollectionExists())
            {
                DeserializePackageCollection();
            }
            else
            {
                if (_isInputV3Style)
                {
                    EnumerateFilesV3Style(onlyLatest);
                }
                else
                {
                    // No OnlyLatest option in here yet
                    EnumerateFilesFlatStyle();
                }
                SerializePackageCollection(clearPackageCollectionCache);
            }
        }


        private void GetDownloadsCount(bool clearDownloadsCache)
        {
            if (!clearDownloadsCache && DownloadCountsExist())
            {
                DeserializeDownloadCounts();
            }
            else
            {
                PrimeDownloadCountsCache();
                SerializeDownloadCounts(clearDownloadsCache);
            }
        }

        private void EnumerateFilesV3Style(bool onlyLatest)
        {
            try
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                var packageDirectories = Directory.EnumerateDirectories(_inputPath).ToArray();
                long count = 0;
                long errorCount = 0;
                var logger = new CommandOutputLogger(NuGet.Common.LogLevel.Information);
                ParallelOptions ops = new ParallelOptions { MaxDegreeOfParallelism = _maxThreadCount };
                Parallel.ForEach(packageDirectories, ops, packageDirectory =>
                {

                    count++;
                    if (count % 10000 == 0)
                    {
                        DisplayStats(count, errorCount, packageDirectories.Length, timer);
                    }
                    try
                    {
                        var id = Path.GetFileName(packageDirectory);
                        var idPackages = LocalFolderUtility.GetPackagesV3(_inputPath, id, logger);

                        if (onlyLatest)
                        {
                            var package = idPackages
                                .OrderByDescending(e => e.Identity.Version)
                                .Max();

                            ProcessArchiveForNuGetAPIsUsedInScripts(package);
                        }
                        else
                        {
                            foreach (var package in idPackages)
                            {
                                ProcessArchiveForNuGetAPIsUsedInScripts(package);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        errorCount++;
                        logger.LogInformation("Exception while parsing " + packageDirectory);
                        logger.LogInformation(ex.Message);
                        var curroptFilePath = Path.Combine(@"f:\CurroptPackages", Path.GetFileName(packageDirectory));
                        File.Move(packageDirectory, curroptFilePath);
                    }
                });
                logger.LogInformation("Done with all the " + count + " packages");
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

        private void EnumerateFilesFlatStyle()
        {
            try
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                var inputFiles = Directory.EnumerateFiles(_inputPath).ToArray();
                long count = 0;
                long errorCount = 0;
                var logger = new CommandOutputLogger(NuGet.Common.LogLevel.Information);
                ParallelOptions ops = new ParallelOptions { MaxDegreeOfParallelism = _maxThreadCount };
                Parallel.ForEach(inputFiles, ops, file =>
                {
                    count++;
                    if (count % 10000 == 0)
                    {
                        DisplayStats(count, errorCount, inputFiles.Length, timer);
                    }
                    try
                    {
                        //ProcessArchiveForNuGetAPIsUsedInScripts(file);
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

        private void DisplayStats(long count, long errorCount, int fileCount, Stopwatch timer)
        {
            Console.WriteLine("Done with " + count + " packages with " + errorCount + " errors");
            double percentDone = (double)count / (double)fileCount;
            var timeLeft = ((timer.Elapsed.TotalSeconds / count) * fileCount) - timer.Elapsed.TotalSeconds;
            var dateEnd = DateTime.Now.AddSeconds(timeLeft);
            var timeLeftSpan = TimeSpan.FromSeconds(timeLeft);
            Console.WriteLine($"Done in {timeLeftSpan} at {dateEnd}");
        }

        private void SerializePackageCollection(bool clearPackageCollectionCache)
        {
            CreateFile(_packageCollectionFilePath, deleteIfExists: clearPackageCollectionCache);
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

        private void SerializeDownloadCounts(bool clearDownloadsCache)
        {
            CreateFile(_downloadCountsFilePath, deleteIfExists: clearDownloadsCache);
            var json = JsonConvert.SerializeObject(_downloadCounts, Formatting.Indented);
            //write to _packageCollectionFilePath
            File.WriteAllText(_downloadCountsFilePath, json);

            CreateFile(_downloadCountsOverIdFilePath, deleteIfExists: clearDownloadsCache);
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

        private void ProcessArchiveForPPFiles(string path)
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

        private void ProcessArchiveForNuGetAPIsUsedInScripts(LocalPackageInfo packageInfo)
        {
            using (var archive = ZipFile.Open(packageInfo.Path, ZipArchiveMode.Read))
            {
                var scriptFiles = archive.Entries
                    .Where(e => e.FullName.EndsWith(".ps1", StringComparison.OrdinalIgnoreCase) || e.FullName.EndsWith(".psm1", StringComparison.OrdinalIgnoreCase));

                if (scriptFiles.Count() > 0)
                {
                    var packageIdentity = packageInfo.Identity;
                    var pathResolver = new VersionFolderPathResolver(_outputPath);
                    var writeDir = Path.Combine(_outputPath, pathResolver.GetPackageDirectory(packageIdentity.Id, packageIdentity.Version));
                    Directory.CreateDirectory(writeDir);

                    foreach (var scriptFile in scriptFiles)
                    {
                        var path = Path.Combine(writeDir, Guid.NewGuid().ToString() + scriptFile.Name);
                        scriptFile.ExtractToFile(path, true);

                        using (var stream = scriptFile.Open())
                        using (var reader = new StreamReader(stream))
                        {
                            var scriptFileContent = reader.ReadToEnd();
                            if (scriptFileContent.Contains("NuGet.VisualStudio.IFileSystemProvider") ||
                                scriptFileContent.Contains("NuGet.VisualStudio.ISolutionManager"))
                            {
                                AddToPackageCollection(packageIdentity, scriptFile.FullName, "");
                            }
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
                    _packageCollection[id] = new Dictionary<string, List<string>>
                    {
                        [fileKey] = new List<string> { version }
                    };
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

        private void GenerateResults()
        {
            GenerateIdFileVersionsCsv();
            GenerateUniqueIdDownloadCountCsv();
            GenerateIdLatestVersionCsv();
        }


        private void GenerateIdFileVersionsCsv()
        {           
            var outputFilePath = Path.Combine(_logPath, @"PackageId_File_Versions.csv");
            CreateFile(outputFilePath, deleteIfExists: true);
            Console.WriteLine($"Generating Id -> File -> Versions CSV file at {outputFilePath}");
            using (StreamWriter w = File.AppendText(outputFilePath))
            {
                w.WriteLine("Package Id, File Path, Package Versions");
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        var versionList = _packageCollection[id][fileKey].ToArray();
                        var delimiter = "  ";
                        var versionString = versionList.Aggregate((i, j) => i + delimiter + j);
                        w.WriteLine(string.Concat(id, ",", fileKey, ",", versionString));
                    }
                }
            }
        }

        private void GenerateIdLatestVersionCsv()
        {
            var outputFilePath = Path.Combine(_logPath, @"PackageId_File_LatestVersion.csv");
            CreateFile(outputFilePath, deleteIfExists: true);
            Console.WriteLine($"Generating Id -> File -> LatestVersions CSV file at {outputFilePath}");
            using (StreamWriter w = File.AppendText(outputFilePath))
            {
                w.WriteLine("Package Id, File Path, Latest Package Version");
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        var latestVersion = _packageCollection[id][fileKey]
                                            .Select(x => new NuGetVersion(x))
                                            .OrderByDescending(version => version)
                                            .Max();

                        w.WriteLine(string.Concat(id, ",", fileKey, ",", latestVersion.ToNormalizedString()));
                    }
                }
            }
        }

        private void GenerateUniqueIdDownloadCountCsv()
        {
            var outputFilePath = Path.Combine(_logPath, @"PackageId_TotalDownloadCount.csv");
            CreateFile(outputFilePath, deleteIfExists: true);
            Console.WriteLine($"Generating Id -> TotalDownloadCount CSV file at {outputFilePath}");
            using (StreamWriter w = File.AppendText(outputFilePath))
            {
                w.WriteLine("Package Id, Total Download Count");
                foreach (var id in _packageCollection.Keys)
                {
                    var idDownloadCount = GetDownloadCountFromIdCache(id);
                    w.WriteLine(id + "," + idDownloadCount);
                }
            }
        }

        private void GenerateIdVersionDownloadCountCsv()
        {
            var outputFilePath = Path.Combine(_logPath, @"PackageId_Version_DownloadCount.csv");
            CreateFile(outputFilePath, deleteIfExists: true);
            Console.WriteLine($"Generating Id -> Version -> DownloadCount CSV file at {outputFilePath}");
            var seen = new HashSet<string>();
            using (StreamWriter w = File.AppendText(outputFilePath))
            {
                w.WriteLine("Package Id, Package Version, Download Count");
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

        private void GenerateIdLatestVersionDownloadCountCsv()
        {
            var outputFilePath = Path.Combine(_logPath, @"PackageId_LatestVersion_DownloadCount.csv");
            CreateFile(outputFilePath, deleteIfExists: true);
            Console.WriteLine($"Generating Id -> LatestVersion -> DownloadCount CSV file at {outputFilePath}");
            var seen = new HashSet<string>();
            using (StreamWriter w = File.AppendText(outputFilePath))
            {
                w.WriteLine("Package Id, Package Latest Version, Download Count");
                foreach (var id in _packageCollection.Keys)
                {
                    foreach (var fileKey in _packageCollection[id].Keys)
                    {
                        var latestVersion = _packageCollection[id][fileKey]
                                            .Select(x => new NuGetVersion(x))
                                            .OrderByDescending(version => version)
                                            .Max();
                        var count = GetDownloadCount(id, latestVersion.ToNormalizedString());
                        var dataString = string.Concat(id + "," + latestVersion.ToNormalizedString() + "," + count);
                        if (!seen.Contains(dataString))
                        {
                            seen.Add(dataString);
                            w.WriteLine(dataString);
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
                return jsonResponse;
            }
        }

        private long GetDownloadCount(string id, string version)
        {
            PrimeDownloadCountsCache(id);
            return GetDownloadCountFromCache(id, version);
        }

        private long GetDownloadCount(string id)
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