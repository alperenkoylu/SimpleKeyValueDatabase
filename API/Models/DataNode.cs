namespace API.Models;

public struct DataNode(string? data, bool tombStone)
{
    public string? Value { get; set; } = data;
    public bool TombStone { get; set; } = tombStone;
}
