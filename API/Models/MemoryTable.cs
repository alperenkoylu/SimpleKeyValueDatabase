using System.Text.Json;

namespace API.Models;

public class MemoryTable
{
    public Dictionary<string, DataNode>? DataNodes { get; private set; } = [];

    public int Size => DataNodes.Count;

    public MemoryTable(string writeAheadLogPath)
    {
        if (File.Exists(writeAheadLogPath))
        {
            string writeAheadLogText = File.ReadAllText(writeAheadLogPath);

            if (writeAheadLogText.Length > 0)
            {
                DataNodes = JsonSerializer.Deserialize<Dictionary<string, DataNode>>(writeAheadLogText);
            }
        }
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

    public void Add(string key, string value)
    {
        DataNodes[key] = new DataNode { Value = value, TombStone = false };
    }

    public void Delete(string key)
    {
        DataNodes[key] = new DataNode { Value = null, TombStone = true };
    }

    public void Clear()
    {
        DataNodes.Clear();
    }
}
