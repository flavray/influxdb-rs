use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug)]
pub enum Field {
    Boolean(bool),
    Float(f64),
    Integer(i64),
    String(String),
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}",
            match *self {
                Field::Boolean(true) => "t".to_string(),
                Field::Boolean(false) => "f".to_string(),
                Field::Float(value) => format!("{}", value),
                Field::Integer(value) => format!("{}i", value),
                Field::String(ref value) => value.clone(),
            }
        )
    }
}

#[derive(Clone, Debug)]
pub struct Point<'a> {
    pub key: &'a str,
    pub timestamp: Option<i64>,
    pub tags: HashMap<&'a str, &'a str>,
    pub fields: HashMap<&'a str, Field>,
}

impl<'a> Point<'a> {
    pub fn new(key: &'a str) -> Point<'a> {
        Point {
            key: key,
            timestamp: None,
            tags: HashMap::new(),
            fields: HashMap::new(),
        }
    }

    pub fn timestamp(mut self, timestamp: i64) -> Point<'a> {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn tag(mut self, name: &'a str, value: &'a str) -> Point<'a> {
        self.tags.insert(name, value);
        self
    }

    pub fn field(mut self, name: &'a str, value: Field) -> Point<'a> {
        self.fields.insert(name, value);
        self
    }

    pub fn serialize(&self) -> String {
        let mut result = String::new();
        result += self.key;

        for (tag, value) in &self.tags {
            result += &format!(",{}={}", tag, value);
        }

        result += " ";

        result += &self.fields
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join(",");

        match self.timestamp {
            Some(timestamp) => result += &format!(" {}", timestamp),
            None => (),
        }

        result
    }
}

#[derive(Clone, Debug)]
pub struct BatchPoints<'a> {
    pub points: Vec<Point<'a>>,
    pub database: &'a str,
}

impl<'a> BatchPoints<'a> {
    pub fn new(database: &'a str) -> BatchPoints<'a> {
        BatchPoints {
            points: Vec::new(),
            database: database,
        }
    }

    pub fn add_point(mut self, point: Point<'a>) -> BatchPoints<'a> {
        self.points.push(point);
        self
    }

    pub fn add_points(mut self, points: Vec<Point<'a>>) -> BatchPoints<'a> {
        self.points.extend(points);
        self
    }

    pub fn one(database: &'a str, point: Point<'a>) -> BatchPoints<'a> {
        BatchPoints::new(database)
            .add_point(point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point() {
        let point = Point::new("cpu_usage")
            .timestamp(1000)
            .tag("cpu", "cpu-total")
            .field("idle", Field::Float(89.3))
            .field("busy", Field::Float(10.7));

        assert_eq!("cpu_usage", point.key);
        assert_eq!(1, point.tags.keys().collect::<Vec<_>>().len());
        assert_eq!(2, point.fields.keys().collect::<Vec<_>>().len());
    }
}
