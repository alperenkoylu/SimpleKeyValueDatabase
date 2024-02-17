using System.IO.Compression;
using API.Models;

var builder = WebApplication.CreateBuilder(args);

ConfigurationManager configuration = builder.Configuration;

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

int memoryTableSizeLimit;

if (!int.TryParse(Environment.GetEnvironmentVariable("MEM_TABLE_SIZE_LIMIT"), out memoryTableSizeLimit))
{
    memoryTableSizeLimit = configuration.GetValue<int>("memoryTableSizeLimit");
}

int ssTablesCountLimit;

if (!int.TryParse(Environment.GetEnvironmentVariable("SS_TABLE_COUNT_LIMIT"), out ssTablesCountLimit))
{
    ssTablesCountLimit = configuration.GetValue<int>("ssTablesCountLimit");
}

string KeyValueDatabaseDataPath = "wwwroot/data";

if (!Directory.Exists(KeyValueDatabaseDataPath))
{
    Directory.CreateDirectory(KeyValueDatabaseDataPath);
}

KeyValueDatabase keyValueDatabase = new(KeyValueDatabaseDataPath, memoryTableSizeLimit, ssTablesCountLimit);

builder.Services.AddSingleton(provider => keyValueDatabase);

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

RouteGroupBuilder keyValueDatabaseRouteGroup = app.MapGroup("api/keyvaluedatabase").WithTags("Key Value Database");

keyValueDatabaseRouteGroup.MapGet("{key}", (string key, KeyValueDatabase database) =>
{
    string? value = database.Get(key);

    if (value is null)
    {
        return Results.NotFound();
    }

    return Results.Ok(value);
})
.WithSummary("Get Record")
.WithOpenApi();


keyValueDatabaseRouteGroup.MapPost("", (string key, string value, KeyValueDatabase database) =>
{
    database.Add(key, value);

    return Results.Created($"/api/database/{key}", value);
})
.WithSummary("Add or Update Record")
.WithOpenApi();

keyValueDatabaseRouteGroup.MapDelete("{key}", (string key, KeyValueDatabase database) =>
{
    database.Delete(key);
    return Results.NoContent();
})
.WithSummary("Delete Record")
.WithOpenApi();

keyValueDatabaseRouteGroup.MapGet("sstables", (KeyValueDatabase database) =>
{
    if (Directory.Exists(KeyValueDatabaseDataPath))
    {
        string ZipFileName = string.Concat(DateTime.Now.ToString("yyyyMMddHHmmss"), "_SSTables.zip");
        string ZipFilePath = Path.Combine(Path.GetTempPath(), ZipFileName);

        using (var zip = new ZipArchive(File.Create(ZipFilePath), ZipArchiveMode.Create))
        {
            List<string> SSTableFiles = Directory.GetFiles(KeyValueDatabaseDataPath, "*.json").OrderBy(fileName => fileName).ToList();

            foreach (string SSTableFileName in SSTableFiles)
            {
                ZipArchiveEntry entry = zip.CreateEntry(Path.GetFileName(SSTableFileName));

                using Stream entryStream = entry.Open();
                using FileStream fileStream = File.OpenRead(SSTableFileName);

                fileStream.CopyTo(entryStream);
            }
        }

        return Results.File(ZipFilePath, "application/zip", ZipFileName);
    }

    return Results.NotFound();
})
.WithSummary("Download SSTables as Zip File")
.WithOpenApi();

app.Run();