package algo;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import path.RelationshipTypeAndDirections;
import result.PathResult;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PathFinding {
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    private PathExpander<Double> buildPathExpanderLongs(String relationshipsAndDirections, String propertyName, long min, long max) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections
                .parse(relationshipsAndDirections)) {
            if (pair.first() == null) {
                if (pair.other() == null) {
                    builder = PathExpanderBuilder.allTypesAndDirections().addRelationshipFilter(relationship ->
                            ((long) relationship.getProperty(propertyName)) >= min && ((long) relationship.getProperty(propertyName)) <= max
                    );;
                } else {
                    builder = PathExpanderBuilder.allTypes(pair.other()).addRelationshipFilter(relationship ->
                            ((long) relationship.getProperty(propertyName)) >= min && ((long) relationship.getProperty(propertyName)) <= max
                    );;
                }
            } else {
                if (pair.other() == null) {
                    builder = builder.add(pair.first()).addRelationshipFilter(relationship ->
                            ((long) relationship.getProperty(propertyName)) >= min && ((long) relationship.getProperty(propertyName)) <= max
                    );;
                } else {
                    builder = builder.add(pair.first(), pair.other())
                            .addRelationshipFilter(relationship ->
                            ((long) relationship.getProperty(propertyName)) >= min && ((long) relationship.getProperty(propertyName)) <= max
                    );
                }
            }
        }
        return builder.build();
    }

    @Procedure
    @Description("algo.allSimplePathsFiltered(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 5, propertyName, min, max) YIELD path, " +
            "weight - run allSimplePaths with relationships given and maxNodes")
    public Stream<PathResult> allSimplePathsFiltered(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("maxNodes") long maxNodes,
            @Name("propertyName") String propertyName,
            @Name("minValue") long minValue,
            @Name("maxValue") long maxValue) {

        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                new BasicEvaluationContext(tx, db),
                buildPathExpanderLongs(relTypesAndDirs, propertyName, minValue, maxValue),
                (int) maxNodes
        );
        Iterable<Path> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(PathResult::new);
    }

    private PathExpander<Double> buildPathExpanderDateTimes(String relationshipsAndDirections, String propertyName, ZonedDateTime min, ZonedDateTime max) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections
                .parse(relationshipsAndDirections)) {
            if (pair.first() == null) {
                if (pair.other() == null) {
                    builder = PathExpanderBuilder.allTypesAndDirections().addRelationshipFilter(relationship ->
                            ((ZonedDateTime) relationship.getProperty(propertyName)).isAfter(min) && ((ZonedDateTime) relationship.getProperty(propertyName)).isBefore(max)
                    );;
                } else {
                    builder = PathExpanderBuilder.allTypes(pair.other()).addRelationshipFilter(relationship ->
                            ((ZonedDateTime) relationship.getProperty(propertyName)).isAfter(min) && ((ZonedDateTime) relationship.getProperty(propertyName)).isBefore(max)
                    );;
                }
            } else {
                if (pair.other() == null) {
                    builder = builder.add(pair.first()).addRelationshipFilter(relationship ->
                            ((ZonedDateTime) relationship.getProperty(propertyName)).isAfter(min) && ((ZonedDateTime) relationship.getProperty(propertyName)).isBefore(max)
                    );;
                } else {
                    builder = builder.add(pair.first(), pair.other())
                            .addRelationshipFilter(relationship ->
                                    ((ZonedDateTime) relationship.getProperty(propertyName)).isAfter(min) && ((ZonedDateTime) relationship.getProperty(propertyName)).isBefore(max)
                            );
                }
            }
        }
        return builder.build();
    }


    @Procedure
    @Description("algo.allSimplePathsFilteredDt(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 5, minDt, maxDt) YIELD path, " +
            "weight - run allSimplePaths with relationships given and maxNodes")
    public Stream<PathResult> allSimplePathsFilteredDt(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("maxNodes") long maxNodes,
            @Name("propertyName") String propertyName,
            @Name("minValue") ZonedDateTime minValue,
            @Name("maxValue") ZonedDateTime maxValue) {


        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                new BasicEvaluationContext(tx, db),
                buildPathExpanderDateTimes(relTypesAndDirs, propertyName, minValue, maxValue),
                (int) maxNodes
        );
        Iterable<Path> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(PathResult::new);
    }
    
}


