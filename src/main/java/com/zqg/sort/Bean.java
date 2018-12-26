package com.zqg.sort;

import com.sun.corba.se.spi.servicecontext.ServiceContextData;
import scala.Int;
import scala.math.Ordered;


import java.io.Serializable;


public class Bean  implements Serializable, Ordered<Bean> {
    private  Integer first;
    private Integer second;

    public Bean() {
    }

    public Bean(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public int compare(Bean that) {
        return 0;
    }

    /**
     * 小于
     * @param that
     * @return
     */
    @Override
    public boolean $less(Bean that) {

        if(this.first<that.first)
        {
            return  true;
        }
        if(this.first==that.first&& this.second<that.second)
        {
            return  true;
        }

        return  false;


    }

    /**
     * 参数大于当前的对象
     * @param that
     * @return
     */

    @Override
    public boolean $greater(Bean that) {

        if(this.first>that.first)
        {
            return  true;
        }
        if(this.first==that.first&& this.second>that.second)
        {
            return  true;
        }

        return  false;




    }

    @Override
    public boolean $less$eq(Bean that) {

        if (this.$less(that))
        {
            return  true;
        }
        if(this.first==that.first&& that.second==that.second)
        {
            return  true;

        }
        return  false;

    }

    @Override
    public boolean $greater$eq(Bean that) {

        if(this.$greater(that))
        {
            return  true;
        }

        if(this.first==that.first&& that.second==that.second)
        {
            return  true;

        }
        return  false;
    }

    @Override
    public int compareTo(Bean that) {
      if(this.first!=that.first)
      {
          return this.first-that.first;
      }
      else  {
          return  this.second-that.second;
      }


    }

    @Override
    public String toString() {
        return "Bean{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
