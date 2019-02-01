package org.mitre.synthea.engine;

import com.google.common.reflect.ClassPath;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ClassPathUtils;
import org.mitre.synthea.modules.CardiovascularDiseaseModule;
import org.mitre.synthea.modules.EncounterModule;
import org.mitre.synthea.modules.HealthInsuranceModule;
import org.mitre.synthea.modules.LifecycleModule;
import org.mitre.synthea.modules.QualityOfLifeModule;
import org.mitre.synthea.world.agents.Person;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Module represents the entry point of a generic module.
 * 
 * <p>The `modules` map is the static list of generic modules. It is loaded once per process, 
 * and the list of modules is shared between the generated population. Because we share modules 
 * across the population, it is important that States are cloned before they are executed. 
 * This keeps the "master" copy of the module clean.
 */
public class Module {

  private static final Map<String, Module> modules = loadModules();

  private static Map<String, Module> loadModules() {
    Map<String, Module> retVal = new ConcurrentHashMap<String, Module>();

    retVal.put("Lifecycle", new LifecycleModule());
    retVal.put("Cardiovascular Disease", new CardiovascularDiseaseModule());
    retVal.put("Quality Of Life", new QualityOfLifeModule());
    retVal.put("Health Insurance", new HealthInsuranceModule());

    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    try {
        Resource[] resources = resolver.getResources("/modules/**/*.json");
        Resource modulesDir = resolver.getResource("/modules");
        for (Resource resource : resources) {
            Module module = loadResource(resource, modulesDir);
            String relativePath = relativePath(resource, modulesDir);
            retVal.put(relativePath, module);
        }
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.format("Loaded %d modules.\n", retVal.size());

    return retVal;
  }

  private static String relativePath(Resource file, Resource modulesDir) {
    try {
      Pattern p = Pattern.compile("^" + Pattern.quote(modulesDir.getURL().toExternalForm()) + "/(?<relativePath>.+)\\.json$");
      Matcher m = p.matcher(file.getURL().toExternalForm());
      if (!m.matches()) {
        throw new RuntimeException("Path matcher didn't match"); // this should only happen if there's a bug in the above regex
      }
      return m.group("relativePath");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Module loadResource(Resource resource, Resource modulesDir) throws Exception {
    System.out.format("Loading %s\n", resource.getDescription());
    boolean submodule = resource.createRelative(".").equals(modulesDir);
    try(
        JsonReader reader = new JsonReader(new InputStreamReader(resource.getInputStream()))
    ) {
        JsonParser parser = new JsonParser();
        JsonObject object = parser.parse(reader).getAsJsonObject();
      return new Module(object, submodule);
    }
  }

  public static String[] getModuleNames() {
    return modules.keySet().toArray(new String[modules.size()]);
  }

  /**
   * Get the top-level modules.
   * @return a list of top-level modules. Submodules are not included.
   */
  public static List<Module> getModules() {
    List<Module> list = new ArrayList<Module>();
    modules.forEach((k, v) -> {
      if (!v.submodule) {
        list.add(v);
      }
    });
    return list;
  }

  /**
   * Get a module by path.
   * @param path
   *          : the relative path of the module, without the root or ".json" file extension. For
   *          example, "medications/otc_antihistamine" or "appendicitis".
   * @return module : the given module
   */
  public static Module getModuleByPath(String path) {
    return modules.get(path);
  }

  public String name;
  public boolean submodule;
  public List<String> remarks;
  private Map<String, State> states;

  protected Module() {
    // no-args constructor only allowed to be used by subclasses
  }

  public Module(JsonObject definition, boolean submodule) throws Exception {
    name = String.format("%s Module", definition.get("name").getAsString());
    this.submodule = submodule;
    remarks = new ArrayList<String>();
    if (definition.has("remarks")) {
      JsonElement jsonRemarks = definition.get("remarks");
      for (JsonElement value : jsonRemarks.getAsJsonArray()) {
        remarks.add(value.getAsString());
      }
    }

    JsonObject jsonStates = definition.get("states").getAsJsonObject();
    states = new ConcurrentHashMap<String, State>();
    for (Entry<String, JsonElement> entry : jsonStates.entrySet()) {
      State state = State.build(this, entry.getKey(), entry.getValue().getAsJsonObject());
      states.put(entry.getKey(), state);
    }
  }

  /**
   * Process this Module with the given Person at the specified time within the simulation.
   * 
   * @param person
   *          : the person being simulated
   * @param time
   *          : the date within the simulated world
   * @return completed : whether or not this Module completed.
   */
  @SuppressWarnings("unchecked")
  public boolean process(Person person, long time) {
    person.history = null;
    // what current state is this person in?
    if (!person.attributes.containsKey(this.name)) {
      person.history = new LinkedList<State>();
      person.history.add(initialState());
      person.attributes.put(this.name, person.history);
    }
    person.history = (List<State>) person.attributes.get(this.name);
    String activeKey = EncounterModule.ACTIVE_WELLNESS_ENCOUNTER + " " + this.name;
    if (person.attributes.containsKey(EncounterModule.ACTIVE_WELLNESS_ENCOUNTER)) {
      person.attributes.put(activeKey, true);
    }
    State current = person.history.get(0);
    // System.out.println(" Resuming at " + current.name);
    // process the current state,
    // looping until module is finished,
    // probably more than one state
    String nextStateName = null;
    while (current.run(person, time)) {
      Long exited = current.exited;      
      nextStateName = current.transition(person, time);
      // System.out.println(" Transitioning to " + nextStateName);
      current = states.get(nextStateName).clone(); // clone the state so we don't dirty the original
      person.history.add(0, current);
      if (exited != null && exited < time) {
        // This must be a delay state that expired between cycles, so temporarily rewind time
        process(person, exited);
        current = person.history.get(0);
      }
    }
    person.attributes.remove(activeKey);
    return (current instanceof State.Terminal);
  }

  private State initialState() {
    return states.get("Initial"); // all Initial states have name Initial
  }

  public State getState(String name) {
    return states.get(name);
  }

  /**
   * Get a collection of the names of all the states this Module contains.
   * 
   * @return set of all state names, or empty set if this is a non-GMF module
   */
  public Collection<String> getStateNames() {
    if (states == null) {
      // ex, if this is a non-GMF module
      return Collections.emptySet();
    }
    return states.keySet();
  }
}
