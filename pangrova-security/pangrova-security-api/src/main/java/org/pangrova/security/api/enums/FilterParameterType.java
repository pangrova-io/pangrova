package org.pangrova.security.api.enums;

/** SQL representation of a bound row-filter parameter. */
public enum FilterParameterType {
    /** A single string value rendered as 'value'. */
    STRING,
    /** A comma-separated list of values rendered as 'val1,val2,...'. */
    STRING_LIST
}
