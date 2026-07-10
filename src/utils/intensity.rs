//! 基于震级和震源距估算 JMA 震度

/// 返回 0-7 的整数震度
///
/// 衰减模型为 `I = a * M - b * log10(D + c) + d`，震级分段处会混合两组系数，
/// 避免估算值在边界附近跳变过大
pub fn estimate_intensity(magnitude: f64, distance_km: f64) -> u8 {
    if !magnitude.is_finite() || !distance_km.is_finite() || magnitude <= 0.0 || distance_km < 0.0 {
        return 0;
    }

    let (a, b, c, d) = intensity_coefficients(magnitude);
    let effective_distance = distance_km.max(1.0);
    let intensity = a * magnitude - b * (effective_distance + c).log10() + d;

    intensity.clamp(0.0, 7.0).round() as u8
}

fn intensity_coefficients(magnitude: f64) -> (f64, f64, f64, f64) {
    let small = (2.5, 3.8, 12.0, -1.2);
    let medium = (2.5, 3.6, 10.0, -1.3);
    let strong = (2.3, 3.7, 10.0, -1.0);
    let major = (2.0, 3.8, 10.0, -0.8);

    if magnitude < 4.8 {
        small
    } else if magnitude < 5.2 {
        blend_coefficients(small, medium, (magnitude - 4.8) / 0.4)
    } else if magnitude < 5.8 {
        medium
    } else if magnitude < 6.2 {
        blend_coefficients(medium, strong, (magnitude - 5.8) / 0.4)
    } else if magnitude < 6.8 {
        strong
    } else if magnitude < 7.2 {
        blend_coefficients(strong, major, (magnitude - 6.8) / 0.4)
    } else {
        major
    }
}

fn blend_coefficients(
    left: (f64, f64, f64, f64),
    right: (f64, f64, f64, f64),
    t: f64,
) -> (f64, f64, f64, f64) {
    (
        lerp(left.0, right.0, t),
        lerp(left.1, right.1, t),
        lerp(left.2, right.2, t),
        lerp(left.3, right.3, t),
    )
}

fn lerp(left: f64, right: f64, t: f64) -> f64 {
    left + (right - left) * t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_intensity() {
        let i1 = estimate_intensity(7.0, 10.0);
        assert!(i1 >= 5);

        let i2 = estimate_intensity(7.0, 100.0);
        assert!(i2 < i1);

        let i3 = estimate_intensity(5.0, 50.0);
        assert!((1..=5).contains(&i3));

        let i4 = estimate_intensity(4.0, 10.0);
        assert!(i4 <= i3);
    }

    #[test]
    fn test_magnitude_boundary_smoothing() {
        let before = estimate_intensity(4.9, 240.0);
        let after = estimate_intensity(5.0, 240.0);
        assert!(after.saturating_sub(before) <= 1);

        assert_eq!(estimate_intensity(5.1, 280.0), 2);
        assert_eq!(estimate_intensity(4.8, 280.0), 1);
    }

    #[test]
    fn rejects_non_finite_inputs() {
        assert_eq!(estimate_intensity(f64::NAN, 10.0), 0);
        assert_eq!(estimate_intensity(5.0, f64::INFINITY), 0);
        assert_eq!(estimate_intensity(f64::INFINITY, 10.0), 0);
    }

    #[test]
    fn near_field_is_continuous_at_one_kilometer() {
        let just_under = estimate_intensity(5.0, 0.99);
        let at_one = estimate_intensity(5.0, 1.0);
        assert!(at_one.abs_diff(just_under) <= 1);
    }
}
