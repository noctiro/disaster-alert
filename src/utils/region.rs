const ADMIN_SUFFIXES: &[&str] = &[
    "特别行政区",
    "自治区",
    "自治州",
    "自治县",
    "地区",
    "盟",
    "省",
    "市",
    "县",
    "区",
];

pub fn normalize(value: &str) -> String {
    let mut value = value.trim().to_lowercase();
    if let Some(suffix) = ADMIN_SUFFIXES
        .iter()
        .find(|suffix| value.ends_with(**suffix) && value.len() > suffix.len())
    {
        value.truncate(value.len() - suffix.len());
    }
    value
}

pub fn equivalent(left: &str, right: &str) -> bool {
    let left = normalize(left);
    let right = normalize(right);
    !left.is_empty() && left == right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_only_trailing_administrative_suffixes() {
        assert_eq!(normalize("广东省"), "广东");
        assert_eq!(normalize("成都市"), "成都");
        assert_eq!(normalize("市中区"), "市中");
        assert!(!equivalent("市中区", "中山区"));
        assert_eq!(normalize("广州市"), "广州");
        assert_eq!(normalize("贵州省"), "贵州");
    }
}
