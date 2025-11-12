# Browser Flow Files

This directory contains browser flow JSON files for automated web scraping.

## Directory Structure

```
browser-flows/
├── lee-county-browser-flow.json
├── miami-dade-county-browser-flow.json
└── ...
```

## Naming Convention

Files should be named: `{county-name}-county-browser-flow.json`
- County name in lowercase
- Spaces replaced with hyphens
- Must end with `-county-browser-flow.json`

Examples:
- `lee-county-browser-flow.json`
- `miami-dade-county-browser-flow.json`
- `palm-beach-county-browser-flow.json`

## Deployment

These files are automatically uploaded to S3 during deployment:
```bash
./scripts/deploy-infra.sh
```

They will be uploaded to:
```
s3://${EnvironmentBucket}/browser-flows/${county-name}-county-browser-flow.json
```

## Browser Flow Format

Browser flow files define the automation steps for web scraping. See the Elephant CLI documentation for the complete format specification.

Example structure:
```json
{
  "steps": [
    {
      "action": "navigate",
      "url": "https://example.com"
    },
    {
      "action": "fill",
      "selector": "#search-input",
      "value": "{{parcel_id}}"
    },
    {
      "action": "click",
      "selector": "#search-button"
    },
    {
      "action": "wait",
      "selector": "#results"
    }
  ]
}
```




