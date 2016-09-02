using System;
using System.IO;
using System.IO.Compression;

namespace Nuget.NupkgParser
{
    internal class NupkgPs1Parser
    {
        private static void Main(string[] args)
        {
            var folderPath = @"F:\MirrorPackages";
            enumerateFiles(folderPath);
        }

        private static void enumerateFiles(string path)
        {
            foreach (string file in Directory.EnumerateFiles(path))
            {
                containsScripts(file);
            }
        }

        private static bool containsScripts(string path)
        {
            bool result = false;
            openArchive(path);
            return result;
        }

        private static void openArchive(string path)
        {
            // Get id and version for nupkg

            // Copy nupkg into v3 style structure

            // Read nupkg for ps1's and extract the nupkgs into the same folder as nupkg
            var archive = ZipFile.Open(path, ZipArchiveMode.Read);
            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.EndsWith(".ps1", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(string.Concat("ps1 - ", entry.Name, " in nupkg - ", Path.GetFileName(path)));
                }
            }
        }
    }
}