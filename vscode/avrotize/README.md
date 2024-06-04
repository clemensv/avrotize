# Avrotize 

*EXPERIMENTAL. USE AT YOUR OWN RISK.*

[Avrotize](https://github.com/clemensv/avrotize) is a
["Rosetta Stone"](https://en.wikipedia.org/wiki/Rosetta_Stone) for data
structure definitions, allowing you to convert between numerous data and
database schema formats and to generate code for different programming
languages.

It is, for instance, a well-documented and predictable converter and code
generator for data structures originally defined in JSON Schema (of arbitrarys
complexity).

The tool leans on the Apache Avro-derived
["Avrotize Schema"](https://github.com/clemensv/avrotize/blob/master/specs/avrotize-schema.md) as its schema
model.


![Screenshot](https://raw.githubusercontent.com/clemensv/avrotize/master/vscode/avrotize/media/screenshot.png)

## Features

The Visual Studio Code extension for Avrotize provides a user-friendly interface
to the Avrotize tool, allowing you to convert between different data structure
formats and generate code for different programming languages. You will find the
conversion commands in the context menu of the explorer view.

## Requirements

The Avrotize tool must be installed on your system and available in the path.
Avrotize requires Python 3.11 or later to be installed on your system first. 

You can then install Avrotize using pip, or have the extension install the tool
for you when you first use it. 

```bash
pip install avrotize
```

If you only want to run the tool in a virtual environment, you can create one,
install Avrotize in it, and then start Visual Studio Code from within the
virtual environment context.

Once Avrotize is installed, the extension will detect whether the Avrotize 
version is current and will automatically update it to match the extension
version if necessary.

## Using the Avrotize Extension

Once the Avrotize extension is installed, you can use it to convert different file formats to and from Avro schemas directly from the VS Code explorer context menu. The extension will add a new "Convert to" menu item with subitems based on the file type you are working with. Below is a guide to what commands are available for which file types and what happens when you select them.

### Supported File Types and Available Commands

#### From Avrotize Schema

| File Extension | Context Menu Command                | Description                                                              | Steps                                                                                                                                                                                               |
| -------------- | ----------------------------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `.avsc`        | Convert to > Parquet                | Converts an Avro schema to a Parquet file and saves the output.          | - Provide output file name and location                                                                                                                                                             |
| `.avsc`        | Convert to > JSON                   | Converts an Avro schema to a JSON file and saves the output.             | - Provide output file name and location                                                                                                                                                             |
| `.avsc`        | Convert to > XML                    | Converts an Avro schema to an XML file and saves the output.             | - Provide output file name and location                                                                                                                                                             |
| `.avsc`        | Convert to > SQL Schema             | Converts an Avro schema to a SQL schema and saves the output.            | - Select the SQL dialect<br>- Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                              |
| `.avsc`        | Convert to > MongoDB Schema         | Converts an Avro schema to a MongoDB schema and saves the output.        | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > Cassandra Schema       | Converts an Avro schema to a Cassandra schema and saves the output.      | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > DynamoDB Schema        | Converts an Avro schema to a DynamoDB schema and saves the output.       | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > Elasticsearch Schema   | Converts an Avro schema to an Elasticsearch schema and saves the output. | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > CouchDB Schema         | Converts an Avro schema to a CouchDB schema and saves the output.        | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > Neo4j Schema           | Converts an Avro schema to a Neo4j schema and saves the output.          | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > Firebase Schema        | Converts an Avro schema to a Firebase schema and saves the output.       | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > CosmosDB Schema        | Converts an Avro schema to a CosmosDB schema and saves the output.       | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > HBase Schema           | Converts an Avro schema to an HBase schema and saves the output.         | - Decide if you want to add CloudEvents columns<br>- Provide output file name and location                                                                                                          |
| `.avsc`        | Convert to > C# Classes             | Converts an Avro schema to C# classes and saves the output.              | - Provide C# root namespace<br>- Decide if you want to use Avro annotations<br>- Decide if you want to use System.Text.Json annotations<br>- Provide output file name and location                  |
| `.avsc`        | Convert to > Java Classes           | Converts an Avro schema to Java classes and saves the output.            | - Decide if you want to use Avro annotations<br>- Decide if you want to use Jackson annotations<br>- Decide if you want to use PascalCase properties<br>- Provide output file name and location     |
| `.avsc`        | Convert to > Python Classes         | Converts an Avro schema to Python classes and saves the output.          | - Decide if you want to use dataclasses-json annotations<br>- Decide if you want to use Avro annotations<br>- Provide output file name and location                                                 |
| `.avsc`        | Convert to > TypeScript Classes     | Converts an Avro schema to TypeScript classes and saves the output.      | - Decide if you want to use Avro annotations<br>- Decide if you want to use TypedJSON annotations<br>- Provide output file name and location                                                        |
| `.avsc`        | Convert to > JavaScript Classes     | Converts an Avro schema to JavaScript classes and saves the output.      | - Decide if you want to use Avro annotations<br>- Provide output file name and location                                                                                                             |
| `.avsc`        | Convert to > C++ Classes            | Converts an Avro schema to C++ classes and saves the output.             | - Provide root namespace<br>- Decide if you want to use Avro annotations<br>- Decide if you want to use JSON annotations<br>- Provide output file name and location                                 |
| `.avsc`        | Convert to > Go Classes             | Converts an Avro schema to Go classes and saves the output.              | - Decide if you want to use Avro annotations<br>- Decide if you want to use JSON annotations<br>- Provide Go package name, site, and username (optional)<br>- Provide output file name and location |
| `.avsc`        | Convert to > Rust Classes           | Converts an Avro schema to Rust classes and saves the output.            | - Provide Rust package name (optional)<br>- Decide if you want to use Avro annotations<br>- Decide if you want to use JSON annotations<br>- Provide output file name and location                   |
| `.avsc`        | Convert to > Data Pipeline JSON     | Converts an Avro schema to Data Pipeline JSON and saves the output.      | - Provide record type (optional)<br>- Provide output file name and location                                                                                                                         |
| `.avsc`        | Convert to > Markdown Documentation | Converts an Avro schema to Markdown documentation and saves the output.  | - Provide output file name and location                                                                                                                                                             |

#### From Other Formats

| File Extension | Context Menu Command     | Description                                                     | Steps                                                                                                                                 |
| -------------- | ------------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `.parquet`     | Convert to > Avro Schema | Converts a Parquet file to an Avro schema and saves the output. | - Provide namespace for the Avro schema<br>- Provide output file name and location                                                    |
| `.json`        | Convert to > Avro Schema | Converts a JSON file to an Avro schema and saves the output.    | - Provide namespace for the Avro schema<br>- Decide if you want to split top-level records<br>- Provide output file name and location |
| `.xml`         | Convert to > Avro Schema | Converts an XML file to an Avro schema and saves the output.    | - Provide namespace for the Avro schema<br>- Provide output file name and location                                                    |
| `.asn`         | Convert to > Avro Schema | Converts an ASN file to an Avro schema and saves the output.    | - Provide output file name and location                                                                                               |
| `.csv`         | Convert to > Avro Schema | Converts a CSV file to an Avro schema and saves the output.     | - Provide namespace for the Avro schema<br>- Provide output file name and location                                                    |

### Using the Context Menu Commands

1. **Right-Click on a File**: In the VS Code explorer, right-click on a file with a supported extension.
2. **Select "Convert to" Menu**: Hover over the "Convert to" menu item to see the available conversion options for the selected file.
3. **Select the Conversion Option**: Click on the desired conversion option.
4. **Provide Output Details**: You will be prompted to provide details such as the output file name and location. Fill in the necessary information.
5. **View Output**: The conversion process will run, and the output will be saved to the specified location. If the output is a file, it will be opened in the editor. If it is a directory, a new window will be opened with the directory.

By following these steps, you can easily convert between various data formats and Avro schemas using the Avrotize extension in Visual Studio Code.


## Release Notes

Refer to the Avrotize repository for the release notes of the Avrotize tool.
