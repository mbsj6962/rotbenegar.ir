/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Utilities;

/**
 *
 * @author root
 */
public class Configuration implements java.io.Serializable {
    public String log_type;
    public String rank_formula;
    public String data_path;
    public String input_dir_in_data_path;
    public String input_data_path;
    public String geo_file_path;
    public String fraud_file_path;
    public String test_mode;
    
    public String getLogType() {
        return log_type;
    }
    
    public String getRankFormula() {
        return rank_formula;
    }
    
    public String getDataPath() {
        return data_path;
    }
    
    public String getRelativeInputDataPath() {
        return input_dir_in_data_path;
    }
    
    public String getAbsoluteInputDataPath() {
        return input_data_path;
    }
    
    public String getGeoFilePath() {
        return geo_file_path;
    }
    
    public String getFraudFilePath() {
        return fraud_file_path;
    }
    
    public String getTestMode() {
        return test_mode;
    }
}
