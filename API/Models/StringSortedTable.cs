using System.Text.Json;

namespace API.Models;

public class StringSortedTable
{
    public SortedDictionary<string, DataNode> DataNodes { get; private set; } = [];

    public StringSortedTable() { }

    public StringSortedTable(string filePath)
    {
        LoadFromDisk(filePath);
    }

    public StringSortedTable(string dataDirectory, MemoryTable memoryTable)
    {
        LoadFromMemoryTable(memoryTable);
        WriteToDisk(dataDirectory);
    }

    public void Add(string key, DataNode value)
    {
        DataNodes[key] = value;
    }

    public bool Get(string key, out string? value)
    {
        value = null;

        var DataExists = DataNodes.TryGetValue(key, out DataNode Node);

        if (!DataExists)
        {
            return false;
        }

        if (Node.TombStone)
        {
            return false;
        }

        value = Node.Value;
        return true;
    }

    public void LoadFromMemoryTable(MemoryTable memoryTable)
    {
        foreach (var Node in memoryTable.DataNodes)
        {
            DataNodes.Add(Node.Key, Node.Value);
        }
    }

    public void WriteToDisk(string dataDirectory)
    {
        string fileName = string.Concat(DateTime.Now.ToString("yyyyMMddHHmmss"), "-SSTable.json");

        string data = JsonSerializer.Serialize(DataNodes, new JsonSerializerOptions
        {
            WriteIndented = true,
        });

        File.WriteAllText(Path.Combine(dataDirectory, fileName), data);
    }

    public void LoadFromDisk(string filePath)
    {
        string rawData = File.ReadAllText(filePath);

        DataNodes = JsonSerializer.Deserialize<SortedDictionary<string, DataNode>>(rawData)!;
    }
}
