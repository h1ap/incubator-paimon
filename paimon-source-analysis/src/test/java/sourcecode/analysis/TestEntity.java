package sourcecode.analysis;

public class TestEntity {
    public Integer pk;
    public String name;
    public Integer age;
    public Integer dt;

    public TestEntity(Integer pk, String name, Integer age, Integer dt) {
        this.pk = pk;
        this.name = name;
        this.age = age;
        this.dt = dt;
    }

    public Integer getPk() {
        return pk;
    }

    public void setPk(Integer pk) {
        this.pk = pk;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getDt() {
        return dt;
    }

    public void setDt(Integer dt) {
        this.dt = dt;
    }

    @Override
    public String toString() {
        return "TestEntity{" +
                "pk=" + pk +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", dt=" + dt +
                '}';
    }
}
