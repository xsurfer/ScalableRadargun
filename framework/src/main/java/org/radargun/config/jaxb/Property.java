//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vhudson-jaxb-ri-2.1-833 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2009.11.10 at 06:49:47 PM EET 
//


package org.radargun.config.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for anonymous complex type.
 * <p/>
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p/>
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="mapAggregator" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="value" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "")
@XmlRootElement(name = "property")
public class Property {

   @XmlAttribute
   @XmlJavaTypeAdapter(Adapter1.class)
   protected String mapAggregator;
   @XmlAttribute(required = true)
   @XmlJavaTypeAdapter(Adapter1.class)
   protected String name;
   @XmlAttribute(required = true)
   @XmlJavaTypeAdapter(Adapter1.class)
   protected String value;

   /**
    * Gets the value of the mapAggregator property.
    *
    * @return possible object is {@link String }
    */
   public String getMapAggregator() {
      return mapAggregator;
   }

   /**
    * Sets the value of the mapAggregator property.
    *
    * @param value allowed object is {@link String }
    */
   public void setMapAggregator(String value) {
      this.mapAggregator = value;
   }

   /**
    * Gets the value of the name property.
    *
    * @return possible object is {@link String }
    */
   public String getName() {
      return name;
   }

   /**
    * Sets the value of the name property.
    *
    * @param value allowed object is {@link String }
    */
   public void setName(String value) {
      this.name = value;
   }

   /**
    * Gets the value of the value property.
    *
    * @return possible object is {@link String }
    */
   public String getValue() {
      return value;
   }

   /**
    * Sets the value of the value property.
    *
    * @param value allowed object is {@link String }
    */
   public void setValue(String value) {
      this.value = value;
   }

}
