"""
Cloud cost calculator for SQL query optimization.
Estimates AWS RDS costs based on execution time, I/O operations, and storage.
"""
import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()


class CostCalculator:
    """
    Calculates estimated cloud database costs based on query performance.
    Uses simplified AWS RDS pricing model.
    """

    def __init__(self):
        """Initialize cost calculator with pricing from environment variables."""
        self.cost_per_vcpu_hour = float(os.getenv("COST_PER_VCPU_HOUR", "0.10"))
        self.cost_per_gb_storage_month = float(os.getenv("COST_PER_GB_STORAGE_MONTH", "0.10"))
        self.cost_per_million_iops = float(os.getenv("COST_PER_MILLION_IOPS", "0.20"))

    def estimate_query_cost(
        self,
        execution_time_ms: float,
        rows_scanned: int = 0,
        rows_returned: int = 0,
        table_size_mb: float = 1.0,
        has_index: bool = False
    ) -> Dict[str, Any]:
        """
        Estimate the cost of running a query.
        
        Args:
            execution_time_ms: Query execution time in milliseconds
            rows_scanned: Number of rows scanned (approximation)
            rows_returned: Number of rows returned
            table_size_mb: Size of table(s) involved in MB
            has_index: Whether query uses an index
            
        Returns:
            Dictionary with cost breakdown and total
        """
        # Convert execution time to hours
        execution_hours = execution_time_ms / (1000 * 60 * 60)

        # CPU cost (based on execution time)
        cpu_cost = execution_hours * self.cost_per_vcpu_hour

        # I/O cost (based on rows scanned)
        # Assume each row scan is 1 I/O operation
        # Without index: Full table scan
        # With index: Reduced I/O (assume 10x improvement)
        if has_index:
            io_operations = max(rows_returned * 10, 100)  # Minimum overhead
        else:
            io_operations = max(rows_scanned, rows_returned * 100)

        io_cost = (io_operations / 1_000_000) * self.cost_per_million_iops

        # Storage cost (prorated per query)
        # Assume 1 month = 720 hours
        storage_cost_per_query = (table_size_mb / 1024) * self.cost_per_gb_storage_month * execution_hours / 720

        # Total cost
        total_cost = cpu_cost + io_cost + storage_cost_per_query

        return {
            "total_cost_usd": round(total_cost, 8),
            "cpu_cost_usd": round(cpu_cost, 8),
            "io_cost_usd": round(io_cost, 8),
            "storage_cost_usd": round(storage_cost_per_query, 8),
            "io_operations": io_operations,
            "execution_time_ms": execution_time_ms
        }

    def calculate_savings(
        self,
        baseline_cost: Dict[str, Any],
        optimized_cost: Dict[str, Any],
        queries_per_day: int = 1000
    ) -> Dict[str, Any]:
        """
        Calculate cost savings from optimization.
        
        Args:
            baseline_cost: Cost breakdown for baseline query
            optimized_cost: Cost breakdown for optimized query
            queries_per_day: Estimated queries per day
            
        Returns:
            Dictionary with savings breakdown
        """
        cost_per_query_saved = baseline_cost["total_cost_usd"] - optimized_cost["total_cost_usd"]
        
        if cost_per_query_saved <= 0:
            return {
                "cost_per_query_saved_usd": 0.0,
                "daily_savings_usd": 0.0,
                "monthly_savings_usd": 0.0,
                "annual_savings_usd": 0.0,
                "savings_percentage": 0.0
            }

        daily_savings = cost_per_query_saved * queries_per_day
        monthly_savings = daily_savings * 30
        annual_savings = daily_savings * 365

        savings_percentage = (cost_per_query_saved / baseline_cost["total_cost_usd"]) * 100

        return {
            "cost_per_query_saved_usd": round(cost_per_query_saved, 8),
            "daily_savings_usd": round(daily_savings, 6),
            "monthly_savings_usd": round(monthly_savings, 4),
            "annual_savings_usd": round(annual_savings, 2),
            "savings_percentage": round(savings_percentage, 2)
        }

    def estimate_speedup_factor(
        self,
        baseline_time_ms: float,
        optimized_time_ms: float
    ) -> float:
        """
        Calculate speedup factor from optimization.
        
        Args:
            baseline_time_ms: Baseline execution time
            optimized_time_ms: Optimized execution time
            
        Returns:
            Speedup factor (e.g., 2.5 = 2.5x faster)
        """
        if optimized_time_ms <= 0:
            return 1.0
        
        return baseline_time_ms / optimized_time_ms

    def generate_cost_report(
        self,
        baseline_cost: Dict[str, Any],
        optimized_cost: Dict[str, Any],
        speedup_factor: float
    ) -> str:
        """
        Generate human-readable cost comparison report.
        
        Args:
            baseline_cost: Baseline query cost breakdown
            optimized_cost: Optimized query cost breakdown
            speedup_factor: Performance improvement factor
            
        Returns:
            Formatted cost report string
        """
        savings = self.calculate_savings(baseline_cost, optimized_cost)

        report = []
        report.append("=" * 60)
        report.append("COST ANALYSIS REPORT")
        report.append("=" * 60)
        report.append("")
        report.append("PERFORMANCE:")
        report.append(f"  Baseline Execution Time: {baseline_cost['execution_time_ms']:.2f} ms")
        report.append(f"  Optimized Execution Time: {optimized_cost['execution_time_ms']:.2f} ms")
        report.append(f"  Speedup Factor: {speedup_factor:.2f}x")
        report.append("")
        report.append("COST PER QUERY:")
        report.append(f"  Baseline: ${baseline_cost['total_cost_usd']:.8f}")
        report.append(f"  Optimized: ${optimized_cost['total_cost_usd']:.8f}")
        report.append(f"  Savings: ${savings['cost_per_query_saved_usd']:.8f} ({savings['savings_percentage']:.2f}%)")
        report.append("")
        report.append("PROJECTED SAVINGS (1000 queries/day):")
        report.append(f"  Daily: ${savings['daily_savings_usd']:.6f}")
        report.append(f"  Monthly: ${savings['monthly_savings_usd']:.4f}")
        report.append(f"  Annual: ${savings['annual_savings_usd']:.2f}")
        report.append("")
        report.append("I/O OPERATIONS:")
        report.append(f"  Baseline: {baseline_cost['io_operations']:,}")
        report.append(f"  Optimized: {optimized_cost['io_operations']:,}")
        report.append("=" * 60)

        return "\n".join(report)
