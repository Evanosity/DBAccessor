package ca.elixa.db;

public enum DataType {
    String,  Curve,

    Number, Decimal,

    Boolean,

    Key, KeyList,

    //Maps
    StringKeyMap,
    StringStringMap,
    StringNumberMap,
    StringDecimalMap,

    EmbeddedEntity, EmbeddedEntityList
}
