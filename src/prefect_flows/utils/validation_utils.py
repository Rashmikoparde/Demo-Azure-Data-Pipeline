import json
from datetime import datetime

def save_validation_report(validation_report: dict, output_path: str = "validation_reports"):
    """Save validation report to JSON file."""
    import os
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"validation_report_{timestamp}.json"
    filepath = os.path.join(output_path, filename)
    
    with open(filepath, 'w') as f:
        json.dump(validation_report, f, indent=2)
    
    return filepath

def generate_validation_summary(validation_report: dict) -> str:
    """Generate human-readable validation summary."""
    summary = validation_report['summary']
    
    return f"""
📊 VALIDATION SUMMARY
────────────────────
✅ Overall Success: {summary['success']}
📈 Success Rate: {summary['success_rate']:.1f}%
🎯 Quality Score: {summary['data_quality_score']}/100
✔️  Passed: {summary['successful_expectations']}
❌ Failed: {summary['failed_expectations_count']}
📋 Total Checks: {summary['total_expectations']}

💡 Recommendations:
{"".join([f"• {rec}\n" for rec in validation_report['recommendations'][:3]])}
"""