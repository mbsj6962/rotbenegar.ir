/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Utilities;
import eu.bitwalker.useragentutils.*;
import java.io.Serializable;
/**
 *
 * @author bagheri
 */


public class UserAgent implements Serializable
{
	
	private static final long serialVersionUID = 7025462762784240212L;
	private OperatingSystem operatingSystem;
	private Browser browser;
	private int id;
	private String userAgentString;
		
	
	/**
	 * This constructor is created for APIs that require default constructor 
	 * and should never be used directly.
	 * @deprecated Use {@link #UserAgent(OperatingSystem, Browser)} 
	 */
	@Deprecated
	public UserAgent() 
	{
		this(OperatingSystem.UNKNOWN, Browser.UNKNOWN);
	}
	
	public UserAgent(OperatingSystem operatingSystem, Browser browser)
	{
		this.operatingSystem = operatingSystem;
		this.browser = browser;
		this.id = (( operatingSystem.getId() << 16) + browser.getId());
	}
	
    public UserAgent(String userAgentString)
    {
        String userAgentLowercaseString = userAgentString == null ? null : userAgentString.toLowerCase();
        Browser browser = Browser.parseUserAgentLowercaseString(userAgentLowercaseString);

        OperatingSystem operatingSystem = OperatingSystem.UNKNOWN;

        // BOTs don't have an interesting OS for us
        if (browser != Browser.BOT)
            operatingSystem = OperatingSystem.parseUserAgentLowercaseString(userAgentLowercaseString);

        this.operatingSystem = operatingSystem;
        this.browser = browser;
        this.id = ((operatingSystem.getId() << 16) + browser.getId());
        this.userAgentString = userAgentString;
    }

	
	/**
	 * @param userAgentString
	 * @return UserAgent
	 */
	public static UserAgent parseUserAgentString(String userAgentString) {		
		return new UserAgent(userAgentString);
	}
	

	/**
	 * Detects the detailed version information of the browser. Depends on the userAgent to be available. 
	 * Use it only after using UserAgent(String) or UserAgent.parseUserAgent(String). 
	 * Returns null if it can not detect the version information.
	 * @return Version
	 */
	public Version getBrowserVersion() {
		return this.browser.getVersion(this.userAgentString);
	}
	
	/**
	 * @return the system
	 */
	public OperatingSystem getOperatingSystem() {
		return operatingSystem;
	}

	/**
	 * @return the browser
	 */
	public Browser getBrowser() {
		return browser;
	}

	/**
	 * Returns an unique integer value of the operating system & browser combination
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * Combined string representation of both enums
	 */
	public String toString() {
		return this.operatingSystem.toString() + "-" + this.browser.toString();
	}
	
	/**
	 * Returns UserAgent based on specified unique id
	 * @param id
	 * @return
	 */
	public static UserAgent valueOf(int id)
	{
		OperatingSystem operatingSystem = OperatingSystem.valueOf((short) (id >> 16));
		Browser browser = Browser.valueOf( (short) (id & 0x0FFFF));
		return new UserAgent(operatingSystem,browser);
	}
	
	/**
	 * Returns UserAgent based on combined string representation
	 * @param name
	 * @return
	 */
	public static UserAgent valueOf(String name)
	{
		if (name == null)
            throw new NullPointerException("Name is null");
		
		String[] elements = name.split("-");
		
		if (elements.length == 2)
		{
			OperatingSystem operatingSystem = OperatingSystem.valueOf(elements[0]);
			Browser browser = Browser.valueOf(elements[1]);
			return new UserAgent(operatingSystem,browser);
		}
		
		throw new IllegalArgumentException(
	            "Invalid string for userAgent " + name);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((browser == null) ? 0 : browser.hashCode());
		result = prime * result + id;
		result = prime * result
				+ ((operatingSystem == null) ? 0 : operatingSystem.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final UserAgent other = (UserAgent) obj;
		if (browser == null) {
			if (other.browser != null)
				return false;
		} else if (!browser.equals(other.browser))
			return false;
		if (id != other.id)
			return false;
		if (operatingSystem == null) {
			if (other.operatingSystem != null)
				return false;
		} else if (!operatingSystem.equals(other.operatingSystem))
			return false;
		return true;
	}	
	
}