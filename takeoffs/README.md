# House Plans Takeoff Estimator

A simple Python script to extract dimensions from house plan PDFs and generate material takeoff estimates.

## Features

- Extracts square footage from house plan PDFs
- Identifies floor areas, doors, and windows
- Calculates material quantities for:
  - Concrete (foundation, driveway, sidewalk)
  - Lumber (framing, sheathing)
  - Roofing (shingles, underlayment, flashing)
  - Insulation
  - Drywall
  - Flooring (tile, carpet, hardwood)
  - Doors and windows
- Generates cost estimates with labor markup
- Exports results to JSON

## Setup

1. Create a virtual environment:
```bash
python3 -m venv takeoff_env
source takeoff_env/bin/activate  # On Windows: takeoff_env\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Place your house plan PDF in the takeoffs directory and run:

```bash
python takeoff_estimator.py
```

The script will:
1. Extract dimensions from the PDF
2. Calculate material quantities
3. Generate cost estimates
4. Export results to `takeoff_estimate.json`
5. Display a summary

## Example Output

For a 2,430 sqft house plan:
- **Total Materials**: $149,648.31
- **Labor (50% markup)**: $74,824.15
- **TOTAL ESTIMATE**: $224,472.46

## Notes

- This is a prototype tool for basic takeoff estimation
- Material costs are estimates and should be verified with current market prices
- The script works best with PDFs that have clear text-based dimension labels
- Results are exported to JSON for further analysis or integration

## Dependencies

- PyPDF2: PDF text extraction
- pdfplumber: Advanced PDF parsing
- opencv-python: Image processing (for future enhancements)
- pillow: Image handling
- numpy: Numerical computations
- pandas: Data manipulation
- matplotlib: Plotting (for future enhancements)
