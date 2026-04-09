"""
Reward calculation logic for SQL optimization environment.
Implements weighted reward system with partial progress signals.
"""
from typing import Dict, Any
from src.models import Reward, RewardBreakdown


class RewardCalculator:
    """
    Calculates rewards based on multiple components:
    - Grade score (40% weight)
    - Performance improvement (30% weight)
    - Cost savings bonus (20% weight)
    - Safety/Correctness (10% bonus / -50% penalty)
    """

    def __init__(self):
        """Initialize reward calculator with weights."""
        self.grade_weight = 0.40
        self.performance_weight = 0.30
        self.cost_weight = 0.20
        self.safety_weight = 0.10

    def calculate_reward(
        self,
        grade_score: float,
        baseline_time_ms: float,
        optimized_time_ms: float,
        baseline_cost_usd: float,
        optimized_cost_usd: float,
        results_match: bool,
        has_errors: bool = False
    ) -> Reward:
        """
        Calculate total reward with detailed breakdown.
        
        Args:
            grade_score: Grader score (0.0 to 1.0)
            baseline_time_ms: Baseline query execution time
            optimized_time_ms: Optimized query execution time
            baseline_cost_usd: Baseline query cost
            optimized_cost_usd: Optimized query cost
            results_match: Whether results match original query
            has_errors: Whether there were execution errors
            
        Returns:
            Reward object with score, breakdown, and feedback
        """
        # Component 1: Grade score (40% weight)
        grade_component = grade_score * self.grade_weight

        # Component 2: Performance improvement (30% weight)
        performance_component = self._calculate_performance_component(
            baseline_time_ms,
            optimized_time_ms
        )

        # Component 3: Cost savings bonus (20% weight)
        cost_component = self._calculate_cost_component(
            baseline_cost_usd,
            optimized_cost_usd
        )

        # Component 4: Safety/Correctness
        safety_component = self._calculate_safety_component(results_match, has_errors)

        # Calculate performance improvement factor for breakdown
        speedup_factor = 1.0
        if optimized_time_ms > 0 and baseline_time_ms > 0:
            speedup_factor = baseline_time_ms / optimized_time_ms

        # Calculate cost savings for breakdown
        cost_savings = max(0.0, baseline_cost_usd - optimized_cost_usd)

        # Create breakdown
        breakdown = RewardBreakdown(
            grade_score=grade_score,
            performance_improvement=speedup_factor,
            cost_savings_bonus=cost_component,
            correctness_penalty=safety_component if safety_component < 0 else 0.0,
            safety_bonus=safety_component if safety_component >= 0 else 0.0
        )

        # Calculate total score
        total_score = (
            grade_component +
            performance_component +
            cost_component +
            safety_component
        )

        # Clamp to strict (0, 1) for validator compatibility.
        total_score = max(0.01, min(0.99, total_score))

        # Generate feedback
        feedback = self._generate_feedback(
            grade_score,
            speedup_factor,
            cost_savings,
            results_match,
            has_errors,
            total_score
        )

        # Determine if episode is done
        done = total_score >= 0.8 or has_errors

        return Reward(
            score=total_score,
            breakdown=breakdown,
            feedback=feedback,
            done=done
        )

    def _calculate_performance_component(
        self,
        baseline_time_ms: float,
        optimized_time_ms: float
    ) -> float:
        """
        Calculate performance improvement component.
        
        Returns:
            Score between 0.0 and 0.3 (30% weight)
        """
        if optimized_time_ms <= 0 or baseline_time_ms <= 0:
            return 0.0

        speedup = baseline_time_ms / optimized_time_ms

        # Scoring thresholds
        if speedup >= 5.0:
            # Exceptional: 5x+ faster
            return self.performance_weight
        elif speedup >= 3.0:
            # Excellent: 3-5x faster
            return self.performance_weight * 0.9
        elif speedup >= 2.0:
            # Good: 2-3x faster
            return self.performance_weight * 0.75
        elif speedup >= 1.5:
            # Moderate: 1.5-2x faster
            return self.performance_weight * 0.5
        elif speedup >= 1.1:
            # Minimal: 1.1-1.5x faster
            return self.performance_weight * 0.25
        elif speedup >= 0.9:
            # No significant change
            return self.performance_weight * 0.1
        else:
            # Slower than baseline (penalty)
            return -self.performance_weight * 0.5

    def _calculate_cost_component(
        self,
        baseline_cost_usd: float,
        optimized_cost_usd: float
    ) -> float:
        """
        Calculate cost savings component.
        
        Returns:
            Score between 0.0 and 0.2 (20% weight)
        """
        if baseline_cost_usd <= 0:
            return 0.0

        savings_ratio = (baseline_cost_usd - optimized_cost_usd) / baseline_cost_usd

        # Scoring thresholds
        if savings_ratio >= 0.8:
            # Exceptional: 80%+ cost reduction
            return self.cost_weight
        elif savings_ratio >= 0.6:
            # Excellent: 60-80% cost reduction
            return self.cost_weight * 0.9
        elif savings_ratio >= 0.4:
            # Good: 40-60% cost reduction
            return self.cost_weight * 0.75
        elif savings_ratio >= 0.2:
            # Moderate: 20-40% cost reduction
            return self.cost_weight * 0.5
        elif savings_ratio >= 0.1:
            # Minimal: 10-20% cost reduction
            return self.cost_weight * 0.25
        else:
            # No significant savings
            return 0.0

    def _calculate_safety_component(
        self,
        results_match: bool,
        has_errors: bool
    ) -> float:
        """
        Calculate safety/correctness component.
        
        Returns:
            Bonus (+0.1) or penalty (-0.5)
        """
        if has_errors:
            # Critical penalty for SQL errors
            return -0.5

        if results_match:
            # Bonus for maintaining correctness
            return self.safety_weight
        else:
            # Severe penalty for incorrect results
            return -0.5

    def _generate_feedback(
        self,
        grade_score: float,
        speedup_factor: float,
        cost_savings_usd: float,
        results_match: bool,
        has_errors: bool,
        total_score: float
    ) -> str:
        """
        Generate human-readable feedback explaining the score.
        
        Returns:
            Feedback string
        """
        feedback_parts = []

        # Overall assessment
        if total_score >= 0.9:
            feedback_parts.append("🏆 EXCELLENT optimization!")
        elif total_score >= 0.7:
            feedback_parts.append("✓ GOOD optimization")
        elif total_score >= 0.5:
            feedback_parts.append("⚠ MODERATE optimization")
        elif total_score >= 0.0:
            feedback_parts.append("⚠ WEAK optimization")
        else:
            feedback_parts.append("✗ POOR optimization")

        # Grade feedback
        if grade_score >= 0.9:
            feedback_parts.append(f"Grade: {grade_score:.2f} (Excellent)")
        elif grade_score >= 0.7:
            feedback_parts.append(f"Grade: {grade_score:.2f} (Good)")
        elif grade_score >= 0.5:
            feedback_parts.append(f"Grade: {grade_score:.2f} (Moderate)")
        else:
            feedback_parts.append(f"Grade: {grade_score:.2f} (Needs improvement)")

        # Performance feedback
        if speedup_factor >= 3.0:
            feedback_parts.append(f"Performance: {speedup_factor:.2f}x faster ⚡")
        elif speedup_factor >= 2.0:
            feedback_parts.append(f"Performance: {speedup_factor:.2f}x faster ✓")
        elif speedup_factor >= 1.5:
            feedback_parts.append(f"Performance: {speedup_factor:.2f}x faster")
        elif speedup_factor >= 1.0:
            feedback_parts.append(f"Performance: {speedup_factor:.2f}x (minimal improvement)")
        else:
            feedback_parts.append(f"Performance: {speedup_factor:.2f}x (SLOWER than baseline!)")

        # Cost feedback
        if cost_savings_usd > 0:
            feedback_parts.append(f"Cost savings: ${cost_savings_usd:.8f} per query")

        # Correctness feedback
        if has_errors:
            feedback_parts.append("⚠ SQL execution errors detected!")
        elif not results_match:
            feedback_parts.append("⚠ Results do NOT match original query!")
        else:
            feedback_parts.append("✓ Results match (correctness maintained)")

        return " | ".join(feedback_parts)
