# Nexus.Sources.LeosphereWindIris

This data source extension makes it possible to read data files in the Leosphere wind iris format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSources": [
      {
        "Name": "Lidar;real_time",
        "PathSegments": [
          "'Lidar'",
          "yyyy-MM"
        ],
        "FileTemplate": "'WIPO0000000_real_time_data_'yyyy-MM-dd_HH-mm-ss'.csv'",
        "FilePeriod": "00:10:00",
        "UtcOffset": "00:10:00",
        "AdditionalProperties": {
          "SamplePeriod": "00:00:04",
          "Distances": "50, 80, 120, 140, 160, 180, 200, 220, 240, 260, 280, 320, 360, 400, 450, 500, 550, 600, 650, 700"
        }
      },
      {
        "Name": "Lidar;average",
        "PathSegments": [
          "'Lidar'",
          "yyyy-MM"
        ],
        "FileTemplate": "'WIPO0000000_average_data_'yyyy-MM-dd_HH-mm-ss'.csv'",
        "FilePeriod": "1.00:00:00",
        "UtcOffset": "1.00:00:00",
        "AdditionalProperties": {
          "SamplePeriod": "00:10:00",
          "Distances": "50, 80, 120, 140, 160, 180, 200, 220, 240, 260, 280, 320, 360, 400, 450, 500, 550, 600, 650, 700"
        }
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.LeosphereWindIris.Tests) folder for a complete sample.