import de.foam.dataimport.createRowKey
import org.junit.Assert.assertEquals
import org.junit.Test

class HbaseDataImportTest{

    @Test
    fun testCreateRowKey(){
        assertEquals("01_0_0_1",createRowKey("0_0",1))
        assertEquals("00_0_0_30",createRowKey("0_0",30))
        assertEquals("05_0_0_35",createRowKey("0_0",35))
        assertEquals("15_0_0_45",createRowKey("0_0",45))
    }
}