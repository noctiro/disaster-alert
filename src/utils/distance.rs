/// 使用 WGS84 椭球体和 Vincenty 反解公式计算两点距离，返回单位为千米
///
/// 坐标无效或接近对跖点导致迭代不收敛时返回 `None`
#[inline]
pub fn vincenty_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Option<f64> {
    const A: f64 = 6378137.0;
    const B: f64 = 6356752.314245;
    const F: f64 = 1.0 / 298.257223563;
    const A_SQ: f64 = A * A;
    const B_SQ: f64 = B * B;
    const TOLERANCE: f64 = 1e-12;
    const EPSILON: f64 = 1e-24;

    if !lat1.is_finite() || !lat2.is_finite() || !lon1.is_finite() || !lon2.is_finite() {
        return None;
    }

    if !(-90.0..=90.0).contains(&lat1) || !(-90.0..=90.0).contains(&lat2) {
        return None;
    }
    if !(-180.0..=180.0).contains(&lon1) || !(-180.0..=180.0).contains(&lon2) {
        return None;
    }

    let lat_diff = (lat1 - lat2).abs();
    let mut lon_diff = (lon1 - lon2).abs();

    if lon_diff > 180.0 {
        lon_diff = 360.0 - lon_diff;
    }

    if lat_diff < 1e-9 && lon_diff < 1e-9 {
        return Some(0.0);
    }
    if (lat1.abs() - 90.0).abs() < 1e-9 && (lat2.abs() - 90.0).abs() < 1e-9 && lat_diff < 1e-9 {
        return Some(0.0);
    }

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let lon1_rad = lon1.to_radians();
    let lon2_rad = lon2.to_radians();

    let l = lon2_rad - lon1_rad;

    let tan_u1 = (1.0 - F) * lat1_rad.tan();
    let tan_u2 = (1.0 - F) * lat2_rad.tan();
    let u1 = tan_u1.atan();
    let u2 = tan_u2.atan();

    let sin_u1 = u1.sin();
    let cos_u1 = u1.cos();
    let sin_u2 = u2.sin();
    let cos_u2 = u2.cos();

    let mut lambda = l;
    let mut iter_limit = 100;

    let (sin_sigma, cos_sigma, sigma, cos_sq_alpha, cos2_sigma_m) = loop {
        let sin_lambda = lambda.sin();
        let cos_lambda = lambda.cos();

        let term1 = cos_u2 * sin_lambda;
        let term2 = cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda;
        let sin_sq_sigma = term1 * term1 + term2 * term2;

        // 对跖点附近 `sin_sigma` 也会接近 0，不能只靠它判断两点重合
        if sin_sq_sigma < EPSILON && lat_diff < 1e-9 && lon_diff < 1e-6 {
            return Some(0.0);
        }

        if sin_sq_sigma < EPSILON {
            return None;
        }

        let sin_sigma = sin_sq_sigma.sqrt();
        let cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
        let sigma = sin_sigma.atan2(cos_sigma);

        let sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin_sigma;
        let cos_sq_alpha = 1.0 - sin_alpha * sin_alpha;

        let cos2_sigma_m = if cos_sq_alpha != 0.0 {
            cos_sigma - 2.0 * sin_u1 * sin_u2 / cos_sq_alpha
        } else {
            0.0
        };

        let c = F / 16.0 * cos_sq_alpha * (4.0 + F * (4.0 - 3.0 * cos_sq_alpha));
        let cos2_sigma_m_sq = cos2_sigma_m * cos2_sigma_m;

        let lambda_new = l
            + (1.0 - c)
                * F
                * sin_alpha
                * (sigma
                    + c * sin_sigma
                        * (cos2_sigma_m + c * cos_sigma * (-1.0 + 2.0 * cos2_sigma_m_sq)));

        if (lambda_new - lambda).abs() < TOLERANCE {
            break (sin_sigma, cos_sigma, sigma, cos_sq_alpha, cos2_sigma_m);
        }

        lambda = lambda_new;
        iter_limit -= 1;

        if iter_limit == 0 {
            return None;
        }
    };

    let u_sq = cos_sq_alpha * (A_SQ - B_SQ) / B_SQ;

    let u_sq_div_16384 = u_sq / 16384.0;
    let u_sq_div_1024 = u_sq / 1024.0;

    let k1 = u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq));
    let big_a = 1.0 + u_sq_div_16384 * (4096.0 + k1);

    let k2 = u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq));
    let big_b = u_sq_div_1024 * (256.0 + k2);

    let cos2_sigma_m_sq = cos2_sigma_m * cos2_sigma_m;
    let sin_sigma_sq = sin_sigma * sin_sigma;

    let delta_sigma = big_b
        * sin_sigma
        * (cos2_sigma_m
            + 0.25
                * big_b
                * (cos_sigma * (-1.0 + 2.0 * cos2_sigma_m_sq)
                    - big_b / 6.0
                        * cos2_sigma_m
                        * (-3.0 + 4.0 * sin_sigma_sq)
                        * (-3.0 + 4.0 * cos2_sigma_m_sq)));

    let s = B * big_a * (sigma - delta_sigma);

    Some(s / 1000.0)
}

pub fn validate_coordinates(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_point() {
        let dist = vincenty_distance(0.0, 0.0, 0.0, 0.0);
        assert_eq!(dist, Some(0.0));
    }

    #[test]
    fn test_short_distance() {
        let dist = vincenty_distance(39.9042, 116.4074, 31.2304, 121.4737);
        assert!(dist.is_some(), "北京到上海距离应可计算");
        let Some(dist) = dist else {
            return;
        };
        assert!((dist - 1067.0).abs() < 2.0);
    }

    #[test]
    fn test_long_distance() {
        let dist = vincenty_distance(40.6413, -73.7781, 51.4700, -0.4543);
        assert!(dist.is_some(), "纽约到伦敦距离应可计算");
        let Some(dist) = dist else {
            return;
        };
        assert!(
            (dist - 5555.0).abs() < 2.0,
            "Expected ~5555 km, got {} km",
            dist
        );
    }

    #[test]
    fn test_antipodal_points() {
        let dist = vincenty_distance(0.0, 0.0, 0.0, 180.0);

        // Vincenty 在对跖点附近允许不收敛
        if let Some(d) = dist {
            assert!(
                d > 19900.0 && d < 20100.0,
                "Antipodal distance should be ~20000 km, got {} km",
                d
            );
        }
    }

    #[test]
    fn test_near_antipodal() {
        let dist = vincenty_distance(40.4168, -3.7038, -41.2865, 174.7762);

        if let Some(d) = dist {
            assert!(
                d > 19000.0 && d < 20000.0,
                "Near-antipodal distance should be 19000-20000 km, got {} km",
                d
            );
        }
    }

    #[test]
    fn test_invalid_coordinates() {
        assert_eq!(vincenty_distance(91.0, 0.0, 0.0, 0.0), None);
        assert_eq!(vincenty_distance(0.0, 181.0, 0.0, 0.0), None);
        assert_eq!(vincenty_distance(0.0, 0.0, -91.0, 0.0), None);
        assert_eq!(vincenty_distance(0.0, 0.0, 0.0, -181.0), None);
        assert_eq!(vincenty_distance(f64::NAN, 0.0, 0.0, 0.0), None);
        assert_eq!(vincenty_distance(0.0, f64::INFINITY, 0.0, 0.0), None);
    }

    #[test]
    fn test_across_prime_meridian() {
        let dist = vincenty_distance(51.5074, -0.1278, 48.8566, 2.3522);
        assert!(dist.is_some(), "伦敦到巴黎距离应可计算");
        let Some(dist) = dist else {
            return;
        };
        assert!((dist - 344.0).abs() < 2.0);
    }

    #[test]
    fn test_across_date_line() {
        let dist = vincenty_distance(0.0, 179.0, 0.0, -179.0);
        assert!(dist.is_some(), "跨日期变更线距离应可计算");
        let Some(dist) = dist else {
            return;
        };
        assert!(dist < 250.0);
    }

    #[test]
    fn test_validate_coordinates() {
        assert!(validate_coordinates(35.6762, 139.6503));
        assert!(!validate_coordinates(91.0, 0.0));
        assert!(!validate_coordinates(0.0, 181.0));
        assert!(!validate_coordinates(f64::NAN, 0.0));
    }

    #[test]
    fn test_polar_antipodal_not_zero() {
        assert_ne!(vincenty_distance(90.0, 0.0, -90.0, 0.0), Some(0.0));
        assert_eq!(vincenty_distance(90.0, 0.0, 90.0, 120.0), Some(0.0));
    }
}
