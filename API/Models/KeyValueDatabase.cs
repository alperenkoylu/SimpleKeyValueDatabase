using System.Text.Json;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

namespace API.Models;

public class KeyValueDatabase
{
    public readonly string WriteAheadLogFileName = "WriteAheadLog.json";
    public readonly string CompactedSSTableName = "00000000000000-SSTable.json";
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

        DataDirectory = dataDirectory;
        
        InMemoryTable = new MemoryTable(Path.Combine(DataDirectory, WriteAheadLogFileName));
        SSTables = [];
        CompactedSSTable = null;

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

        WriteAhead();

        if (InMemoryTable.Size == MemoryTableSizeLimit)
        {
            Flush();
        }
    }

    public void Delete(string key)
    {
        InMemoryTable.Delete(key);

        WriteAhead();

        if (InMemoryTable.Size >= MemoryTableSizeLimit)
        {
            Flush();
        }
    }

    private void WriteAhead()
    {
        string pathForWriteAheadLog = Path.Combine(DataDirectory, WriteAheadLogFileName);

        if (File.Exists(pathForWriteAheadLog))
        {
            File.Delete(pathForWriteAheadLog);
        }

        string data = JsonSerializer.Serialize(InMemoryTable.DataNodes, new JsonSerializerOptions
        {
            WriteIndented = true,
        });

        File.WriteAllText(pathForWriteAheadLog, data);
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

    private StringSortedTable GetCompactedSSTable()
    {
        StringSortedTable compactedSSTable = new();

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

                compactedSSTable.Add(Node.Key, Node.Value);
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

                compactedSSTable.Add(Node.Key, Node.Value);
                ProcessedKeys.Add(Node.Key);
            }
        }

        return compactedSSTable;
    }

    private void Compaction()
    {
        StringSortedTable tempCompactedSSTable = GetCompactedSSTable();

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
