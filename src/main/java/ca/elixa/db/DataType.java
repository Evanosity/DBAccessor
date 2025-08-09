package ca.elixa.db;

public enum DataType {
    String,  Curve,

    Number, Decimal,

    Boolean,

    Date,

    Key, KeyList,

    //Maps
    StringKeyMap,
    StringStringMap,
    StringNumberMap,
    StringDecimalMap,

    EmbeddedEntity, EmbeddedEntityList
}
