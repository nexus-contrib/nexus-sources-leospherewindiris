# Nexus.Sources.LeosphereWindIris

This data source extension makes it possible to read data files in the Gantner UDBF format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSources": [
      {
        "Name": "group-A",
        "PathSegments": [
          "'group-A'",
          "yyyy-MM"
        ],
        "FileTemplate": "'000__0_'yyyy-MM-dd_HH-mm-ss_000000'.dat'",
        "FilePeriod": "00:10:00",
        "UtcOffset": "00:00:00"
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.LeosphereWindIris.Tests) folder for a complete sample.