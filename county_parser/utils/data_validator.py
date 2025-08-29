"""Data quality validation utilities."""

import polars as pl
from pathlib import Path
from typing import Dict, Any, List, Tuple
from rich.console import Console
from rich.table import Table


class DataQualityValidator:
    """Validate data quality and detect corruption issues."""
    
    def __init__(self):
        self.console = Console()
        
    def validate_row_integrity(self, df: pl.DataFrame, expected_columns: int, 
                              filename: str) -> Dict[str, Any]:
        """Validate that rows have the expected number of columns."""
        
        if df.width != expected_columns:
            self.console.print(f"⚠️  {filename}: Expected {expected_columns} columns, got {df.width}")
            
        # Check for completely empty rows
        empty_rows = df.filter(
            pl.all(pl.col("*").is_null() | (pl.col("*") == ""))
        ).height
        
        # Check for rows with too few non-null values (likely fragmented)
        min_required_fields = max(3, expected_columns // 3)  # At least 1/3 of fields should be non-null
        fragmented_rows = df.filter(
            pl.sum_horizontal([pl.col(c).is_not_null() & (pl.col(c) != "") for c in df.columns]) < min_required_fields
        ).height
        
        return {
            "filename": filename,
            "total_rows": df.height,
            "expected_columns": expected_columns,
            "actual_columns": df.width,
            "empty_rows": empty_rows,
            "fragmented_rows": fragmented_rows,
            "data_integrity_score": 1.0 - (empty_rows + fragmented_rows) / df.height if df.height > 0 else 0
        }
        
    def detect_embedded_newlines(self, file_path: Path, sample_lines: int = 1000) -> Dict[str, Any]:
        """Detect embedded newlines that could cause parsing issues."""
        
        issues = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline().strip()
            expected_tabs = header.count('\t')
            
            for i, line in enumerate(f):
                if i >= sample_lines:
                    break
                    
                tab_count = line.count('\t')
                if tab_count != expected_tabs:
                    issues.append({
                        "line_number": i + 2,  # +2 because we read header first and lines are 1-indexed
                        "expected_tabs": expected_tabs,
                        "actual_tabs": tab_count,
                        "line_preview": line[:100] + "..." if len(line) > 100 else line.strip()
                    })
                    
                    if len(issues) >= 10:  # Limit to first 10 issues
                        break
        
        return {
            "filename": file_path.name,
            "expected_tab_count": expected_tabs,
            "issues_found": len(issues),
            "sample_issues": issues,
            "estimated_problem_rate": len(issues) / min(sample_lines, 1000) if sample_lines > 0 else 0
        }
    
    def generate_quality_report(self, validation_results: List[Dict[str, Any]]) -> None:
        """Generate a comprehensive quality report."""
        
        table = Table(title="📊 Data Quality Report")
        table.add_column("File", style="bold")
        table.add_column("Rows", justify="right")
        table.add_column("Integrity", justify="right")
        table.add_column("Issues", style="red")
        table.add_column("Status", justify="center")
        
        for result in validation_results:
            integrity_pct = result.get("data_integrity_score", 0) * 100
            
            # Determine status
            if integrity_pct >= 98:
                status = "🟢 Excellent"
            elif integrity_pct >= 90:
                status = "🟡 Good"  
            elif integrity_pct >= 75:
                status = "🟠 Fair"
            else:
                status = "🔴 Poor"
                
            issues = []
            if result.get("empty_rows", 0) > 0:
                issues.append(f"{result['empty_rows']} empty")
            if result.get("fragmented_rows", 0) > 0:
                issues.append(f"{result['fragmented_rows']} fragmented")
            if result.get("issues_found", 0) > 0:
                issues.append(f"{result['issues_found']} structure")
                
            issues_str = ", ".join(issues) if issues else "None"
            
            table.add_row(
                result.get("filename", "Unknown"),
                f"{result.get('total_rows', 0):,}",
                f"{integrity_pct:.1f}%",
                issues_str,
                status
            )
        
        self.console.print(table)
        
        # Summary recommendations
        avg_integrity = sum(r.get("data_integrity_score", 0) for r in validation_results) / len(validation_results)
        
        if avg_integrity >= 0.95:
            self.console.print("\n✅ Data quality is excellent. Safe to proceed with full processing.")
        elif avg_integrity >= 0.85:
            self.console.print("\n⚠️  Data quality is good but consider investigating issues before full processing.")
        else:
            self.console.print("\n🚨 Data quality issues detected. Recommend manual inspection before processing large datasets.")
