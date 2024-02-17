using System.IO.Compression;
using System.Reflection;
using API.Models;

var builder = WebApplication.CreateBuilder(args);

ConfigurationManager configuration = builder.Configuration;

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

int memoryTableSizeLimit = configuration.GetValue("memoryTableSizeLimit", 30);
int ssTablesCountLimit = configuration.GetValue("ssTablesCountLimit", 30);
string KeyValueDatabaseDataPath = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot/data");

KeyValueDatabase keyValueDatabase = new(KeyValueDatabaseDataPath, memoryTableSizeLimit, ssTablesCountLimit);

builder.Services.AddSingleton(provider => keyValueDatabase);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

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

//keyValueDatabaseRouteGroup.MapGet("sstables", () =>
//{
//    if (Directory.Exists(KeyValueDatabaseDataPath))
//    {
//        string ZipName = string.Concat(DateTime.Now.ToString("yyyyMMddHHmmss"), "_SSTables.zip");

//        using MemoryStream memoryStream = new();

//        using (ZipArchive zipArchive = new(memoryStream, ZipArchiveMode.Create, true))
//        {
//            List<string> SSTableFiles = Directory.GetFiles(KeyValueDatabaseDataPath, "*.json").OrderBy(fileName => fileName).ToList();

//            foreach (var SSTableFileName in SSTableFiles)
//            {
//                ZipArchiveEntry zipArchiveEntry = zipArchive.CreateEntry(SSTableFileName, CompressionLevel.NoCompression);

//                using Stream entryStream = zipArchiveEntry.Open();

//                memoryStream.CopyTo(entryStream);
//            }
//        }

//        return new File(memoryStream.ToArray(), "application/zip", ZipName);
//    }

//    return Results.NotFound();
//})
//.WithSummary("Download SS Tables as .zip")
//.WithOpenApi();

app.Run();