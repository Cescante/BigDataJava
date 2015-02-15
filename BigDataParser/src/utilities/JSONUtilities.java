package utilities;

import org.json.simple.*;

public class JSONUtilities
{
    public static String SanitizeJSON( String json)
    {
        String ret = json;
        while ( ret.contains( "Infinity" ) )
        {
            ret = ret.replace( "Infinity", "0" );
        }
        while ( ret.contains( "NaN" ) )
        {
            ret = ret.replace( "NaN", "0" );
        }
        
        return ret;
    }
    
    @SuppressWarnings( "unchecked" )
    public static <T> T JSONSimpleGetWithDefault( JSONObject container, String key, T defaultValue )
    {
         Object value = container.get( key );
         
         return value == null ? defaultValue : ( (T)value );
    }
}
