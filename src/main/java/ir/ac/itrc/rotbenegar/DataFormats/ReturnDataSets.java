/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//
package ir.ac.itrc.rotbenegar.DataFormats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ReturnDataSets {

    private Dataset<Row> targetSiteIDAndDomain;
    private Dataset<Row> targetSiteRank;

    public void setTargetSiteIDAndDomain(Dataset<Row> targetSiteIDAndDomain) {

        this.targetSiteIDAndDomain = targetSiteIDAndDomain;

    }

    public Dataset<Row> getTargetSiteIDAndDomain(){
    
    return targetSiteIDAndDomain;
    
    }
    public void setTargetSiteRank(Dataset<Row> targetSiteRank) {

        this.targetSiteRank = targetSiteRank;

    }
    public Dataset<Row> getTargetSiteRank(){
    
    return targetSiteRank;
    
    }
}
//
