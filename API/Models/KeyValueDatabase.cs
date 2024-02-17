using System.Text.Json;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

namespace API.Models;

public class KeyValueDatabase
{
    private readonly string CompactedSSTableName = "00000000000000-SSTable.json";
    private string DataDirectory { get; set; }

    private MemoryTable InMemoryTable { get; set; }

    private List<StringSortedTable> SSTables { get; set; }

    private StringSortedTable? CompactedSSTable { get; set; }

    private int MemoryTableSizeLimit { get; set; } = 5;

    private int SSTablesCountLimit { get; set; } = 5;

    public KeyValueDatabase(string dataDirectory, int memoryTableSizeLimit, int ssTablesCountLimit)
    {
        MemoryTableSizeLimit = memoryTableSizeLimit;
        SSTablesCountLimit = ssTablesCountLimit;

        InMemoryTable = new MemoryTable();
        SSTables = [];
        CompactedSSTable = null;

        DataDirectory = dataDirectory;

        if (Directory.Exists(dataDirectory))
        {
            List<string> SSTableFiles = Directory.GetFiles(dataDirectory, "*SSTable.json").OrderBy(fileName => fileName).ToList();

            foreach (var SSTableFileName in SSTableFiles)
            {
                StringSortedTable tempSSTable = new(SSTableFileName);

                if (SSTableFileName.Equals(CompactedSSTableName))
                {
                    CompactedSSTable = tempSSTable;
                }
                else
                {
                    SSTables.Add(tempSSTable);
                }
            }
        }
        else
        {
            Directory.CreateDirectory(DataDirectory);
        }
    }

    public string? Get(string key)
    {
        string? Result;

        if (InMemoryTable.Get(key, out Result))
        {
            return Result;
        }

        IEnumerable<StringSortedTable> ReversedStringSortedTable = SSTables.Reverse<StringSortedTable>();

        foreach (var SSTable in ReversedStringSortedTable)
        {
            if (SSTable.Get(key, out Result))
            {
                return Result;
            }
        }

        if (CompactedSSTable is not null)
        {
            if (CompactedSSTable.Get(key, out Result))
            {
                return Result;
            }
        }

        return null;
    }

    public void Add(string key, string value)
    {
        InMemoryTable.Add(key, value);

        if (InMemoryTable.Size == MemoryTableSizeLimit)
        {
            Flush();
        }
    }

    public void Delete(string key)
    {
        InMemoryTable.Delete(key);

        if (InMemoryTable.Size >= MemoryTableSizeLimit)
        {
            Flush();
        }
    }

    private void Flush()
    {
        StringSortedTable stringSortedTable = new(DataDirectory, InMemoryTable);

        SSTables.Add(stringSortedTable);

        if (SSTables.Count >= SSTablesCountLimit)
        {
            Compaction();
        }

        InMemoryTable.Clear();
    }

    private void Compaction()
    {
        StringSortedTable tempCompactedSSTable = new();

        HashSet<string> ProcessedKeys = [];

        IEnumerable<StringSortedTable> ReversedStringSortedTable = SSTables.Reverse<StringSortedTable>();

        foreach (var SSTable in ReversedStringSortedTable)
        {
            foreach (var Node in SSTable.DataNodes)
            {
                if (ProcessedKeys.Contains(Node.Key))
                {
                    continue;
                }

                tempCompactedSSTable.Add(Node.Key, Node.Value);
                ProcessedKeys.Add(Node.Key);
            }
        }

        if (CompactedSSTable is not null)
        {
            foreach (var Node in CompactedSSTable.DataNodes)
            {
                if (ProcessedKeys.Contains(Node.Key))
                {
                    continue;
                }

                tempCompactedSSTable.Add(Node.Key, Node.Value);
                ProcessedKeys.Add(Node.Key);
            }
        }

        try
        {
            List<string> SSTableFiles = Directory.GetFiles(DataDirectory, "*SSTable.json").OrderBy(fileName => fileName).ToList();

            foreach (var SSTableFileName in SSTableFiles)
            {
                File.Delete(SSTableFileName);
            }

            string data = JsonSerializer.Serialize(tempCompactedSSTable.DataNodes, new JsonSerializerOptions
            {
                WriteIndented = true,
            });

            File.WriteAllText(Path.Combine(DataDirectory, CompactedSSTableName), data);

            CompactedSSTable = tempCompactedSSTable;
            SSTables.Clear();
        }
        catch
        {

        }
    }
}
